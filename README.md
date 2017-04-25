# YaCy Grid Component: Loader

The YaCy Grid is the second-generation implementation of YaCy, a peer-to-peer search engine.
A YaCy Grid installation consists of a set of micro-services which communicate with each other
using the MCP, see https://github.com/yacy/yacy_grid_mcp

## Purpose

The Loader is a microservice which can be deployed i.e. using Docker.
Each search engine needs a file loader and this component will do that work.
The special feature of this loader is it's embedded headless browser which makes
it possible to load rich content and provide that content for a search engine.

## What it does

When the Loader component is started, it searches for a MCP and connects to it.
By default the local host is searched for a MCP but you can configure one yourself.

The Loader will then wait for client requests and performs web loading upon request.
It also has a MCP queue listener to react on loading requests in the working queues.
After loading of content the loader will push back results to the MCP storage and puts
another message on the MCP message queue to process the loaded content.

## Installation: Download, Build, Run
At this time, yacy_grid_parser is not provided in compiled form, you easily build it yourself. It's not difficult and done in one minute! The source code is hosted at https://github.com/yacy/yacy_grid_loader, you can download it and run loklak with:

    > git clone --recursive https://github.com/yacy/yacy_grid_loader.git

If you just want to make a update, do the following

    > git pull origin master
    > git submodule foreach git pull origin master

To build and start the loader, run

    > cd yacy_grid_loader
    > gradle run

Please read also https://github.com/yacy/yacy_grid_mcp/README.md for further details.

## What is the software license?
LGPL 2.1

Have fun!

@0rb1t3r
