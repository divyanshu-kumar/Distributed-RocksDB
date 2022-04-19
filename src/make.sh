export PATH_OLD=$PATH
export MY_INSTALL_DIR=$HOME/.local
export PATH="$MY_INSTALL_DIR/bin:$PATH"
export PKG_CONFIG_PATH=$MY_INSTALL_DIR/lib/pkgconfig

make

export PATH=$PATH_OLD
