import logging

# Step 2: Configure the logger at the module level
logging.basicConfig(
    level=logging.INFO,  # Set the desired logging level
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    filename="./logs/debug_logs.log",  # Log to a file,
    filemode="w",
)

# Step 3: Inside the function, retrieve or create a logger
logger = logging.getLogger("main_logger")
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())
