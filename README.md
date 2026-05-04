# mux_uart_ssh

## run
venv\Scripts\python.exe mux_uart_ssh.py COM45 --debug

## batch mode
(echo ls;sleep 2) | telnet 192.168.121.243

## development
git --no-pager branch -av
git --no-pager remote show origin
git --no-pager log --oneline --graph --all --decorate
git switch --track origin/uart-tcp-mux-6176508475874039143

