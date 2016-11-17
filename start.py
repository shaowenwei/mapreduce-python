import click, master, os, shutil

# Configure command line options
DEFAULT_NUM_WORKERS = 4
DEFAULT_PORT_NUM = 6000
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

@click.command(context_settings=CONTEXT_SETTINGS)

@click.option("--num_workers", "-w", "num_workers",
    default=DEFAULT_NUM_WORKERS,
    help="Number of workers this server will use, default " + str(DEFAULT_NUM_WORKERS))

@click.option("--port_number", "-p", "port_number",
    default=DEFAULT_PORT_NUM,
    help="The port the master will listen on, default " + str(DEFAULT_PORT_NUM))

def main(num_workers=DEFAULT_NUM_WORKERS, port_number=DEFAULT_PORT_NUM):
  # Remove old tmp dir
  tmp_dir = ".tmp"
  tmp_dir_exists = os.path.isdir(tmp_dir)

  if (tmp_dir_exists):
    shutil.rmtree(tmp_dir)

  # Create new temp directory
  os.mkdir(tmp_dir)

  # Create a new master and let it take over
  master_ = master.Master(num_workers, port_number)

if __name__ == "__main__":
  main()
