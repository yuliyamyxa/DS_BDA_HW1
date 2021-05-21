import sys
from datetime import datetime
from os import path
import os
import argparse
import random


def main():
    parser = argparse.ArgumentParser(description='Generating metrics')
    # CAN NOT BE CONSTANT
    parser.add_argument('--start-date', type=str,
                        help='Start date: dd.mm.yyyy')
    parser.add_argument('--rows', type=int,
                        help='Number of rows')
    parser.add_argument('--devices', type=int,
                        help='Number of devices')
    # CAN BE CONSTANT
    parser.add_argument('--step', type=int,
                        help='Step', default=10)
    parser.add_argument('--output', type=str,
                        help='Output directory', default='../input_data')
    parser.add_argument('--num-files', type=int,
                        help='Number of files with metrics', default=2)
    parser.add_argument('--incorrect', type=int,
                        help='Percent of incorrect data', default=5)

    options = parser.parse_args()

    if not options.rows or not options.devices or not options.start_date:
        print("\n\n", parser.description, "\nSome parameters are not entered correctly."
                                          "\n\nFor more information use '--help'\n")
        sys.exit(1)

    if not os.path.isdir(path.abspath(options.output)):
        print("Created new directory - ./input_data")
        os.mkdir(path.abspath(options.output))

    startDate = int(datetime.strptime(options.start_date + ' 00:00:00', '%d.%m.%Y %H:%M:%S').timestamp())

    millis = startDate
    for num in range(options.num_files):
        with open(path.abspath(path.join(options.output, "metrics_"+str(num+1))), 'w+') as f:
            for i in range(options.rows):
                if random.randint(0, 100) // options.incorrect == 0:
                    f.write("Some incorrect data, 1 \n")
                metricId = random.randint(0, options.devices-1)
                millis = millis + options.step
                value = random.randint(1, 50)
                f.write(str(metricId) + "," + str(millis) + "," + str(value) + "\n")

    with open(path.abspath(path.join(options.output, "devices")), 'w+') as f:
        for idDevice in range(options.devices):
            f.write(str(idDevice) + "-Device_" + str(idDevice) + "\n")


if __name__ == '__main__':
    main()
