"""Test file to demonstrate pre-commit hooks"""

import os
import sys


def badly_formatted_function(  x,y,  z  ):
    """This function has formatting issues"""
    result=x+y+z
    return result


def main():
    print('Using single quotes instead of double')  # Will be caught by ruff
    value = badly_formatted_function(1,2,3)
    print(f"Result: {value}")


if __name__ == '__main__':
    main()
