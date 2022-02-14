import subprocess
import sys
import datetime
import yaml
from config import config
import sys

def run_prob(local = True):
    commands = config.load(local)
    i = 1
    size = len(commands)
    for _ in range(100000):
        for command in commands:
            print("Execute command #{} / {}".format(i, size))
            print('Command: {}'.format(' '.join(command)))
            run(command)
            print("-----------------------")
            i += 1
def run(spark_args):
    with subprocess.Popen(spark_args, stdout = subprocess.PIPE, stderr = subprocess.STDOUT) as p, open('probsky.log', 'ab') as file:
        file.write(str.encode("\n\n-----------------------------------\n{}\n\n".format(' '.join(spark_args))))
        for line in p.stdout: # b'\n'-separated lines
            sys.stdout.buffer.write(line) # pass bytes as is
            file.write(line)
def main(argv):
    local = len(argv) > 0 and argv[0] == '-l'
    run_prob(local)

if __name__ == "__main__":
    main(sys.argv[1:])
