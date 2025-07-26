mod python
mod rust

# list all tasks
@default:
    echo
    echo 'Usage:'
    echo
    echo '    Run `just` to list all tasks.'
    echo '    Run `just <task>` to run a task.'
    echo '    Run `just <python|rust> <task>` to run a task for a specific language.'
    echo
    echo 'Tasks:'
    echo
    just --list --unsorted --list-heading '' --list-submodules
    echo

alias ls := default

# build python + rust
build: python::build rust::build

# test python + rust
test: python::test rust::test

# benchmark rust
benchmark:
    cargo bench

# alias of fmt-fix
fmt: fmt-fix

# fmt check python + rust
fmt-check: python::fmt-check rust::fmt-check

# fmt fix python + rust
fmt-fix: python::fmt-fix rust::fmt-fix

# alias of lint-fix
lint: lint-fix

# lint check python + rust
lint-check: python::lint-check rust::lint-check

# lint fix python + rust
lint-fix: python::lint-fix rust::lint-fix
