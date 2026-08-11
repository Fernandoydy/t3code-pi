# Pi remains an external harness

Pi Agent is installed and authenticated separately and is integrated through the RPC protocol of its `pi` executable. T3 Code owns process lifecycle and protocol translation, but does not import the Pi SDK, bundle Pi with desktop releases, or duplicate Pi authentication and configuration, preserving the product boundary that provider harnesses remain the source of truth for their own behavior.
