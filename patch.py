import re

with open("mux_uart_ssh.py", "r") as f:
    content = f.read()

# Add debug log in _handle_client
content = content.replace(
    """                if not data:
                    break
                # TCP -> UART
                await self._write_to_uart(data)""",
    """                if not data:
                    break
                logging.debug(f"TCP -> UART [{client.peername}]: {repr(data)}")
                # TCP -> UART
                await self._write_to_uart(data)"""
)

# Add debug log in _uart_read_loop
content = content.replace(
    """                    data = await asyncio.to_thread(self._read_serial_data)
                    if data:
                        await self._broadcast_to_clients(data)""",
    """                    data = await asyncio.to_thread(self._read_serial_data)
                    if data:
                        logging.debug(f"UART -> TCP: {repr(data)}")
                        await self._broadcast_to_clients(data)"""
)

# Add --debug argument
content = content.replace(
    """    parser.add_argument("--baud", type=int, default=115200, help="UART baudrate (default: 115200)")

    args = parser.parse_args()""",
    """    parser.add_argument("--baud", type=int, default=115200, help="UART baudrate (default: 115200)")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging to print input characters")

    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)"""
)

with open("mux_uart_ssh.py", "w") as f:
    f.write(content)
