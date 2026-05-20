
PORT:="/dev/serial/by-id/usb-FTDI_FT231X_USB_UART_DUAB9RPU-if00-port0"

assemble:
    flexspin \
        -2 \
        -b \
        -l \
        -q \
        -Wall \
        -Wabs-paths \
        -o example/payload.bin \
        example/payload.spin2

# Loads the application, but does not enter the terminal.
load: assemble
    loadp2 \
        -v \
        -p "{{PORT}}" \
        example/payload.bin

# Loads the application and enters a terminal.
run: assemble
    loadp2 \
        -v \
        -p "{{PORT}}" \
        -t \
        example/payload.bin