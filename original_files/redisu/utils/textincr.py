"""Utility to incrmeent caharcters and strings, which will wrap. For example,
incrmeneting "Z", wraps to "AA" etc. This module provides two functions
  * incr_char
  * incr_str
"""
def incr_char(c):
  """Increment a character, from from 'Z' to 'A'."""
  return chr(ord(c) + 1) if c != 'Z' else 'A'

def incr_str(s):
  """Increment a string, including wrapping from 'Z' to 'AA' etc."""
  lpart = s.rstrip('Z')
  num_replacements = len(s) - len(lpart)
  new_s = lpart[:-1] + incr_char(lpart[-1]) if lpart else 'A'
  new_s += 'A' * num_replacements
  return new_s
