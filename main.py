import time

from config import CONFIG
from src.logger import logger
from src.process_email_inbox import process_email_inbox


def main():
    logger.info("\n" + CONFIG.display_config())

    while True:
        process_email_inbox(
            email_user=CONFIG.EMAIL_ADDRESS,
            email_pass=CONFIG.EMAIL_PASSWORD,
            imap_server=CONFIG.imap_server,
            imap_port=CONFIG.imap_port,
        )
        time.sleep(5)


if __name__ == "__main__":
    main()
