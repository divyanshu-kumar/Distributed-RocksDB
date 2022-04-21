#!/bin/sh
BLUE='\033[0;44m'
NOCOLOR='\033[0m'

echo "${BLUE} Starting ${NOCOLOR}"

echo "${BLUE} Exporting Install Path ${NOCOLOR}"
export MY_INSTALL_DIR=$HOME/.local

echo "${BLUE} Making build directory ${NOCOLOR}"
mkdir -p cmake/build

echo "${BLUE} Entering build directory ${NOCOLOR}"
cd cmake/build

echo "${BLUE} Calling cmake ${NOCOLOR}"
cmake -DCMAKE_PREFIX_PATH=$MY_INSTALL_DIR ../..

echo "${BLUE} Clean build directory ${NOCOLOR}"
make clean

echo "${BLUE} Compiling ${NOCOLOR}"
make -j 8

echo "${BLUE} Done ${NOCOLOR}"