import os
import unittest


def run_tests():
    loader = unittest.TestLoader()
    suite = loader.discover(start_dir=os.getcwd(), pattern='test_*.py')

    runner = unittest.TextTestRunner()
    result = runner.run(suite)

    if result.wasSuccessful():
        print("All tests passed.")
    else:
        exit(1)


if __name__ == '__main__':
    run_tests()
