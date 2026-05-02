#!/usr/bin/env python3
"""
UART to TCP Multiplexer

This script shares a single UART (serial) port across multiple TCP clients concurrently.
It acts as a TCP server, broadcasting any byte received from the UART to all connected
TCP clients and forwarding any data received from any TCP client to the UART.

Requirements:
- Python 3.7+
- pyserial (install via: pip install pyserial)

Usage:
  python3 mux_uart_ssh.py <uart_port> <tcp_listen_port> [--baud <baudrate>]
  python3 mux_uart_ssh.py --list
"""

import argparse
import asyncio
import logging
import sys
import serial
import serial.tools.list_ports

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

class TcpClient:
    """Wrapper to handle individual TCP client queues and write operations."""
    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer
        self.queue = asyncio.Queue(maxsize=4096)  # Prevent infinite memory usage
        self.peername = writer.get_extra_info('peername')
        self.write_task = None

    async def start_writer(self):
        """Background task to write data from queue to the TCP socket."""
        try:
            while True:
                data = await self.queue.get()
                self.writer.write(data)
                await self.writer.drain()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logging.debug(f"Client writer error {self.peername}: {e}")

class UartTcpMultiplexer:
    def __init__(self, uart_port, tcp_port=23, baudrate=115200, telnet=True):
        self.uart_port = uart_port
        self.tcp_port = tcp_port
        self.baudrate = baudrate
        self.telnet = telnet
        self.serial = None
        self.clients = set()
        self.running = False
        self.loop = None
        self.server = None
        self.uart_write_lock = asyncio.Lock()
        self.uart_reader_task = None

    async def start(self):
        self.loop = asyncio.get_running_loop()
        self.running = True

        # Open serial port
        if not self._open_serial():
            logging.error("Failed to open serial port initially. Will attempt reconnects in background.")

        # Start TCP server
        self.server = await asyncio.start_server(
            self._handle_client, '0.0.0.0', self.tcp_port)

        logging.info(f"TCP server listening on 0.0.0.0:{self.tcp_port}")

        # Start UART reader task
        self.uart_reader_task = asyncio.create_task(self._uart_read_loop())

        async with self.server:
            await self.server.serve_forever()

    def _open_serial(self):
        try:
            self.serial = serial.Serial(
                port=self.uart_port,
                baudrate=self.baudrate,
                bytesize=serial.EIGHTBITS,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE,
                timeout=0.1  # Short timeout for non-blocking feel in thread
            )
            logging.info(f"Opened UART {self.uart_port} at {self.baudrate} baud")
            return True
        except Exception as e:
            logging.error(f"Error opening UART {self.uart_port}: {e}")
            return False

    async def _handle_client(self, reader, writer):
        client = TcpClient(reader, writer)
        logging.info(f"Client connected: {client.peername}")

        self.clients.add(client)
        client.write_task = asyncio.create_task(client.start_writer())

        iac_task = None
        if self.telnet:
            async def send_iac_delayed():
                try:
                    await asyncio.sleep(2)
                    telnet_init = b'\xff\xfb\x01\xff\xfb\x03\xff\xfd\x03'
                    client.queue.put_nowait(telnet_init)
                    logging.debug(f"Sent IAC negotiation to [{client.peername}]")
                except asyncio.CancelledError:
                    logging.debug(f"Cancelled IAC negotiation to [{client.peername}]")
            iac_task = asyncio.create_task(send_iac_delayed())

        try:
            import re
            printable_pattern = re.compile(b'[a-zA-Z0-9\r\n]')
            # Match IAC (0xFF) followed by DO/DONT/WILL/WONT (0xFB-0xFE) and an option byte,
            # OR IAC followed by a single command byte.
            iac_pattern = re.compile(b'\xff(?:[\xfb-\xfe].|[\xf0-\xfa])', re.DOTALL)

            while self.running:
                data = await reader.read(4096)
                if not data:
                    break

                # If we received printable chars within the first 2s, cancel IAC
                if iac_task and not iac_task.done():
                    if printable_pattern.search(data):
                        iac_task.cancel()
                        logging.debug(f"Batch mode detected, cancelling IAC for [{client.peername}]")

                # Filter out Telnet IAC sequences from client to UART
                if self.telnet and b'\xff' in data:
                    filtered_data = iac_pattern.sub(b'', data)
                    if data != filtered_data:
                        logging.debug(f"Stripped IAC commands from TCP input. Original: {repr(data)}, Stripped: {repr(filtered_data)}")
                    data = filtered_data

                if not data:
                    continue

                logging.debug(f"TCP -> UART [{client.peername}]: {repr(data)}")
                # TCP -> UART
                await self._write_to_uart(data)
        except ConnectionResetError:
            logging.info(f"Connection reset by client: {client.peername}")
        except Exception as e:
            logging.error(f"Error handling client {client.peername}: {e}")
        finally:
            if 'iac_task' in locals() and iac_task and not iac_task.done():
                iac_task.cancel()
            logging.info(f"Client disconnected: {client.peername}")
            self.clients.discard(client)
            if client.write_task:
                client.write_task.cancel()
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def _write_to_uart(self, data):
        if self.serial and self.serial.is_open:
            try:
                # Use lock to prevent concurrent thread writes and garbled data
                async with self.uart_write_lock:
                    await asyncio.to_thread(self.serial.write, data)
            except serial.SerialException as e:
                logging.error(f"Error writing to UART: {e}")
            except Exception as e:
                logging.error(f"Unexpected error writing to UART: {e}")

    async def _uart_read_loop(self):
        while self.running:
            if self.serial and self.serial.is_open:
                try:
                    # Read using executor to avoid blocking the event loop
                    data = await asyncio.to_thread(self._read_serial_data)
                    if data:
                        logging.debug(f"UART -> TCP: {repr(data)}")
                        await self._broadcast_to_clients(data)
                except serial.SerialException as e:
                    logging.error(f"UART connection lost: {e}")
                    self.serial.close()
                    await self._reconnect_serial()
                except Exception as e:
                    logging.error(f"Unexpected UART read error: {e}")
                    await asyncio.sleep(1)
            else:
                await self._reconnect_serial()

    def _read_serial_data(self):
        """Blocking read called within a background thread."""
        try:
            if self.serial.in_waiting > 0:
                return self.serial.read(self.serial.in_waiting)
            else:
                return self.serial.read(1)
        except AttributeError:
            # Handles edge case during disconnects
            raise serial.SerialException("Port disconnected")
        except Exception:
            raise

    async def _broadcast_to_clients(self, data):
        if not self.clients:
            return

        disconnected = []
        for client in self.clients:
            try:
                # Use put_nowait to immediately return, dropping data if client queue is full
                client.queue.put_nowait(data)
            except asyncio.QueueFull:
                logging.warning(f"Client {client.peername} is too slow, dropping data.")
            except Exception as e:
                logging.debug(f"Broadcast error for {client.peername}: {e}")
                disconnected.append(client)

        for client in disconnected:
            self.clients.discard(client)
            if client.write_task:
                client.write_task.cancel()

    async def _reconnect_serial(self):
        logging.info("Attempting to reconnect to UART...")
        while self.running and not (self.serial and self.serial.is_open):
            if self._open_serial():
                logging.info("Reconnected to UART successfully.")
                break
            await asyncio.sleep(2)

    async def stop(self):
        self.running = False

        if self.uart_reader_task:
            self.uart_reader_task.cancel()

        if self.serial and self.serial.is_open:
            self.serial.close()
            logging.info("UART port closed.")

        if self.server:
            self.server.close()
            await self.server.wait_closed()

        for client in list(self.clients):
            if client.write_task:
                client.write_task.cancel()
            try:
                client.writer.close()
                await client.writer.wait_closed()
            except Exception:
                pass

        self.clients.clear()
        logging.info("Multiplexer stopped cleanly.")

def list_ports():
    ports = serial.tools.list_ports.comports()
    if not ports:
        print("No serial ports found.")
        return
    print("Available serial ports:")
    for port in ports:
        print(f"  - {port.device}: {port.description}")

async def main():
    parser = argparse.ArgumentParser(
        description="UART to TCP Multiplexer",
        epilog="Examples:\n  python3 mux_uart_ssh.py COM42 5555\n  python3 mux_uart_ssh.py /dev/ttyUSB0 2222",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("uart_port", nargs="?", help="UART port (e.g., COM42 or /dev/ttyUSB0)")
    parser.add_argument("tcp_port", nargs="?", type=int, default=23, help="TCP listen port (default: 23)")
    parser.add_argument("--list", action="store_true", help="List available serial ports and exit")
    parser.add_argument("--baud", type=int, default=115200, help="UART baudrate (default: 115200)")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging to print input characters")
    parser.add_argument("--no-telnet", action="store_true", help="Disable Telnet negotiation (sent by default)")

    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    if args.list or (not args.uart_port and not args.tcp_port):
        list_ports()
        sys.exit(0)

    if not args.uart_port or not args.tcp_port:
        parser.print_help()
        sys.exit(1)

    mux = UartTcpMultiplexer(args.uart_port, args.tcp_port, baudrate=args.baud, telnet=not args.no_telnet)

    try:
        await mux.start()
    except asyncio.CancelledError:
        logging.info("Task cancelled. Shutting down.")
    except KeyboardInterrupt:
        pass
    finally:
        await mux.stop()

if __name__ == "__main__":
    if sys.platform == 'win32':
        # Use ProactorEventLoop on Windows for better support of subprocesses/sockets
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProgram interrupted by user. Exiting.")
