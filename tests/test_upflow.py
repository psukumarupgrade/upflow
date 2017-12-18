
import nose.tools as nt  # contains testing tools like ok_, eq_, etc.
import NAME


def setup():
    print("SETUP!")


def teardown():
    print("TEAR DOWN!")


def test_basic():
    print("I RAN!")
