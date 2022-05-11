#!/bin/bash
#/usr/bin/env bash

shopt -s extglob
shopt -s extquote
# shopt -s xpg_echo

set -f

if [[ "$1" == "help" || "$1" == "h" || "$1" == "?" || "$1" == "" ]]; then
    echo
    echo -e "bash $0 [ help/h/? | fmt | check | build | build release | submodules | submodules update | exec | exec_logging | exec_logging_cliout ]\n"

elif [[ "$1" == "fmt" ]]; then

    echo "--check works since cargo-fmt 1.4.38"
    cargo fmt -v --all --check ;

    read -n 1 -s -p "Proceed with cargo fmt? [Enter/y|n] : " choice_fmt
    echo -e "\n"

    if [[ $choice_fmt == "y" || $choice_fmt == "" ]]; then
        cargo fmt -v --all ;
    else
        echo
        echo "Canceled"
    fi

elif [[ "$1" == "check" ]]; then

    cargo check ;

    cargo clippy ;

elif [[ "$1" == "build" && "$2" == "" ]]; then

    cargo build

elif [[ "$1" == "build" && "$2" == "release" ]]; then

    cargo build --release

elif [[ "$1" == "submodules" && "$2" == "" ]]; then

    git submodule status;
    git submodule summary;

elif [[ "$1" == "submodules" && "$2" == "update" ]]; then

    git submodule update --recursive --init

elif [[ "$1" == "exec" ]]; then

    read -n 1 -s -p "Proceed with Banhammer command passing? [Enter/y|n] : " choice_exec
    echo -e "\n"

    if [[ $choice_exec == "y" || $choice_exec == "" ]]; then

        ./target/debug/borealis-banhammer "${@:2}"
        # ./target/release/borealis-banhammer "${@:2}"

    else
        echo
        echo "Canceled"
    fi

elif [[ "$1" == "exec_logging" ]]; then

    read -n 1 -s -p "Proceed with Banhammer command passing? [Enter/y|n] : " choice_exec
    echo -e "\n"

    if [[ $choice_exec == "y" || $choice_exec == "" ]]; then

        sudo mkdir -v -p /var/log/borealis-banhammer/

        sudo chown -v -R $USER:$USER /var/log/borealis-banhammer/

        ./target/debug/borealis-banhammer "${@:2}" >> /var/log/borealis-banhammer/borealis-banhammer.debug.log 2>&1 & disown
        # ./target/release/borealis-banhammer "${@:2}" >> /var/log/borealis-banhammer/borealis-banhammer.release.log 2>&1 & disown

    else
        echo
        echo "Canceled"
    fi

elif [[ "$1" == "exec_logging_cliout" ]]; then

    read -n 1 -s -p "Proceed with Banhammer command passing? [Enter/y|n] : " choice_exec
    echo -e "\n"

    if [[ $choice_exec == "y" || $choice_exec == "" ]]; then

        sudo mkdir -v -p /var/log/borealis-banhammer/

        sudo chown -v -R $USER:$USER /var/log/borealis-banhammer/

        ./target/debug/borealis-banhammer "${@:2}" 2>&1 | tee -a /var/log/borealis-banhammer/borealis-banhammer.debug.log & disown
        # ./target/release/borealis-banhammer "${@:2}" 2>&1 | tee -a /var/log/borealis-banhammer/borealis-banhammer.release.log & disown

    else
        echo
        echo "Canceled"
    fi

fi
