import pytest
from karaoke_shared import pipeline_utils

def test_clean_string_basic():
    assert pipeline_utils.clean_string("test/file.mp3") == "test-file.mp3"
    assert pipeline_utils.clean_string("white space ") == "white space"
    assert pipeline_utils.clean_string("\x00foo") == "foo"

# Add more unit tests for each public function
