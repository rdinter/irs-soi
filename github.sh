#!/bin/bash

git config --global user.email "robert.dinterman@gmail.com"
git config --global user.name "Robert Dinterman"

git config --global credential.helper 'cache --timeout 3600'

git config --global --list