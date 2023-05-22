import os
import torch
import json
import copy
import numpy as np

random_seed = 2317
torch.manual_seed(random_seed)
np.random.seed(random_seed)

from torchvision import datasets, transforms
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
import logging
import random
import model as mdl
from datetime import datetime
import argparse
device = "cpu"
torch.set_num_threads(4)

parser = argparse.ArgumentParser()
parser.add_argument('--master_ip', help='master ip/ip of node 0', type=str)
parser.add_argument('--num_nodes', help='total number of nodes', type=int)
parser.add_argument('--rank', help='rank of current node', type=int)

batch_size = 64 # batch for one node
def train_model(model, train_loader, optimizer, criterion, epoch, lr=0.1):
    """
    model (torch.nn.module): The model created to train
    train_loader (pytorch data loader): Training data loader
    optimizer (optimizer.*): A instance of some sort of optimizer, usually SGD
    criterion (nn.CrossEntropyLoss) : Loss function used to train the network
    epoch (int): Current epoch number
    """
    # remember to exit the train loop at end of the epoch
    running_loss = 0
    for batch_idx, (data, target) in enumerate(train_loader):
        optimizer.zero_grad()
        outputs = model(data)
        loss = criterion(outputs, target)
        loss.backward()

        # we don't want this operation captured in the gradient graph.
        with torch.no_grad():
            for param in model.parameters():
                # access gradient of each layer
                g = torch.as_tensor(param.grad)
                g_list = None
                if torch.distributed.get_rank() == 0:
                    g_list = [torch.zeros(g.size()) for i in range(torch.distributed.get_world_size())]
                # gather the gradients on node with rank 0
                torch.distributed.gather(g, g_list, dst=0)
                if torch.distributed.get_rank() == 0:
                    # compute the mean gradient
                    for i in range(1, len(g_list)):
                        g_list[0] += g_list[i]
                    g_list[0] /= len(g_list)
                    g_list = [g_list[0] for i in range(len(g_list))]
                g_ = torch.zeros(g.size())
                # scatter the computed mean gradient
                torch.distributed.scatter(g_, g_list, src=0)
                # update the gradient
                param.grad.copy_(g_)

        # now update the weights 
        optimizer.step()

        running_loss += loss.item()
        if batch_idx%20 == 19:
            print("[epoch %d, batch %d] loss: %.2f" % (epoch+1, batch_idx+1, running_loss))
            running_loss = 0
        if batch_idx == 0:
            start_timestamp = datetime.now()
        if batch_idx == 39:
            end_timestamp = datetime.now()
            diff = end_timestamp - start_timestamp
            avg_iter_time = diff.total_seconds()/39.0
            print("Average time per iteration after 40 iterations is %.2f secs" % avg_iter_time)

    return None

def test_model(model, test_loader, criterion):
    model.eval()
    test_loss = 0
    correct = 0
    with torch.no_grad():
        for batch_idx, (data, target) in enumerate(test_loader):
            data, target = data.to(device), target.to(device)
            output = model(data)
            test_loss += criterion(output, target)
            pred = output.max(1, keepdim=True)[1]
            correct += pred.eq(target.view_as(pred)).sum().item()

    test_loss /= len(test_loader)
    print('Test set: Average loss: {:.4f}, Accuracy: {}/{} ({:.0f}%)\n'.format(
            test_loss, correct, len(test_loader.dataset),
            100. * correct / len(test_loader.dataset)))


def initialize_distributed_env(master_ip, rank, world_size):
    if not torch.distributed.is_available():
        print("cannot initialise distributed environment.")
        exit()
    if master_ip == None:
        master_ip = 'tcp://10.10.1.1:51001'
    torch.distributed.init_process_group('gloo', \
            init_method=master_ip, \
            world_size=world_size,
            rank=rank)

def main():
    args = parser.parse_args()
    initialize_distributed_env(args.master_ip, args.rank, args.num_nodes)
    normalize = transforms.Normalize(mean=[x/255.0 for x in [125.3, 123.0, 113.9]],
                                std=[x/255.0 for x in [63.0, 62.1, 66.7]])
    transform_train = transforms.Compose([
            transforms.RandomCrop(32, padding=4),
            transforms.RandomHorizontalFlip(),
            transforms.ToTensor(),
            normalize,
            ])

    transform_test = transforms.Compose([
            transforms.ToTensor(),
            normalize])
    training_set = datasets.CIFAR10(root="./data", train=True,
                                                download=True, transform=transform_train)
    sampler = torch.utils.data.distributed.DistributedSampler(training_set, \
            rank=args.rank, num_replicas=args.num_nodes, shuffle=True, seed=random_seed)

    train_loader = torch.utils.data.DataLoader(training_set,
                                                    num_workers=2,
                                                    batch_size=batch_size,
                                                    sampler=sampler,
                                                    pin_memory=True)

    test_set = datasets.CIFAR10(root="./data", train=False,
                                download=True, transform=transform_test)

    test_loader = torch.utils.data.DataLoader(test_set,
                                              num_workers=2,
                                              batch_size=batch_size,
                                              shuffle=False,
                                              pin_memory=True)
    training_criterion = torch.nn.CrossEntropyLoss().to(device)

    model = mdl.VGG11()
    model.to(device)
    optimizer = optim.SGD(model.parameters(), lr=0.1,
                          momentum=0.9, weight_decay=0.0001)
    # running training for one epoch
    for epoch in range(1):
        sampler.set_epoch(epoch)
        epoch_start_timestamp = datetime.now()
        train_model(model, train_loader, optimizer, training_criterion, epoch, 0.1)
        epoch_end_timestamp = datetime.now()
        diff = epoch_end_timestamp - epoch_start_timestamp
        epoch_time = diff.total_seconds()/60.0
        print("Time taken for epoch %d: %.2f mins\n" % (epoch+1, epoch_time))


        test_model(model, test_loader, training_criterion)

if __name__ == "__main__":
    main()
