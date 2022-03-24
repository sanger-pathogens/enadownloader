import pytest

import enadownloader

def test_hello_world(capsys):
    print("Hello World!")
    captured = capsys.readouterr()
    assert captured.out == "Hello World!\n"
    assert captured.err == ""