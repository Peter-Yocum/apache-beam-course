# Python program to demonstrate
# main() function

import apache_beam as beam

from sections.introduction import introduction
from sections.transformations_in_beam import transformations_in_beam


def main():
    print("Starting program...")

    # section 1
    introduction()

    # section 2
    transformations_in_beam()


if __name__ == "__main__":
    main()
