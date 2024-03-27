"""Send data to Graphite metrics server (synchronously or on a background thread).

For example usage, see README.rst.

This code is licensed under a permissive MIT license -- see LICENSE.txt.

The graphyte project lives on GitHub here:
https://github.com/benhoyt/graphyte
"""

# https://github.com/benhoyt/graphyte/blob/master/graphyte.py
# changed behavior if queue is full (would throw an error msg and leave out data)
# now empties the queue and sends the messages if queue runs full

import logging
import queue
import threading
import atexit
import socket
import time


__all__ = ["Sender"]

__version__ = "1.7.1"

logger = logging.getLogger(__name__)


def _has_whitespace(value):
    return not value or value.split(None, 1)[0] != value


class Sender:
    def __init__(
        self,
        host,
        port=2003,
        prefix=None,
        timeout=5,
        interval=None,
        queue_size=None,
        log_sends=False,
        protocol="tcp",
        batch_size=1000,
        tags={},
        raise_send_errors=False,
    ):
        """Initialize a Sender instance, starting the background thread to
        send messages at given interval (in seconds) if "interval" is not
        None. Send at most "batch_size" messages per socket send operation.
        Default protocol is TCP; use protocol='udp' for UDP.

        Use "tags" to specify common or default tags for this Sender, which
        are sent with each metric along with any tags passed to send().
        """

        self.host = host
        self.port = port
        self.prefix = prefix
        self.timeout = timeout
        self.interval = interval
        self.log_sends = log_sends
        self.protocol = protocol
        self.batch_size = batch_size
        self.tags = tags
        self.raise_send_errors = raise_send_errors

        if self.interval is not None:
            if raise_send_errors:
                raise ValueError(
                    "raise_send_errors must be disabled when interval is set"
                )
            if queue_size is None:
                queue_size = int(round(interval)) * 100
            self._queue = queue.Queue(maxsize=queue_size)
            self._thread = threading.Thread(target=self._thread_loop)
            self._thread.daemon = True
            self._thread.start()
            atexit.register(self.stop)

    def __del__(self):
        self.stop()

    def stop(self):
        """Tell the sender thread to finish and wait for it to stop sending
        (should be at most "timeout" seconds).
        """
        if self.interval is not None:
            self._queue.put_nowait(None)
            self._thread.join()
            self.interval = None

    def build_message(self, metric, value, timestamp, tags={}):
        """Build a Graphite message to send and return it as a byte string."""
        if _has_whitespace(metric):
            raise ValueError('"metric" must not have whitespace in it')
        if not isinstance(value, (int, float)):
            raise TypeError(
                '"value" must be an int or a float, not a {}'.format(
                    type(value).__name__
                )
            )

        all_tags = self.tags.copy()
        all_tags.update(tags)
        tags_strs = [";{}={}".format(k, v) for k, v in sorted(all_tags.items())]
        if any(_has_whitespace(t) for t in tags_strs):
            raise ValueError('"tags" keys and values must not have whitespace in them')
        tags_suffix = "".join(tags_strs)

        message = "{}{}{} {} {}\n".format(
            self.prefix + "." if self.prefix else "",
            metric,
            tags_suffix,
            value,
            int(round(timestamp)),
        )
        message = message.encode("utf-8")
        return message

    def send(self, metric, value, timestamp=None, tags={}):
        """Send given metric and (int or float) value to Graphite host.
        Performs send on background thread if "interval" was specified when
        creating this Sender.

        If a "tags" dict is specified, send the tags to the Graphite host along
        with the metric, in addition to any default tags passed to Sender() --
        the tags argument here overrides any default tags.
        """
        message = self.build_message(metric, value, timestamp, tags=tags)

        # removed: if timestamp is None and if interval is None

        try:
            self._queue.put_nowait(message)
        except queue.Full:
            # remember this message, empty the queue and than add the message to queue after queue is emptied
            remember = message
            messages = []
            while True:
                # get all messages from queue and append to messages until queue.Empty
                try:
                    message = self._queue.get_nowait()
                except queue.Empty:
                    # send all messages and break the while true loop
                    for i in range(0, len(messages), self.batch_size):
                        batch = messages[i : i + self.batch_size]
                        msg = b"".join(batch)
                        try:
                            self.send_message(msg)
                        except Exception as E:
                            logger.error(E)
                    break
                else:
                    messages.append(message)
            # append the remembered variable to the next queue
            self._queue.put_nowait(remember)

    def send_message(self, message):
        if self.protocol == "tcp":
            sock = socket.create_connection((self.host, self.port), self.timeout)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            try:
                sock.sendall(message)
            except Exception as E:
                print("ERROR " + str(E))
                logger.error(str(E))
            finally:
                sock.close()

        elif self.protocol == "udp":
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                sock.sendto(message, (self.host, self.port))
            finally:
                sock.close()
        else:
            raise ValueError(
                "\"protocol\" must be 'tcp' or 'udp', not {!r}".format(self.protocol)
            )

    def send_socket(self, message):
        """Low-level function to send message bytes to this Sender's socket.
        You should usually call send() instead of this function (unless you're
        subclassing or writing unit tests).
        """
        try:
            self.send_message(message)
        except Exception as error:
            print(f"Error sending msg to graphite : {error}.")

    def _thread_loop(self):
        """Background thread used when Sender is in asynchronous/interval mode."""
        last_check_time = time.time()
        messages = []
        while True:
            # Get first message from queue, blocking until the next time we
            # should be sending
            time_since_last_check = time.time() - last_check_time
            time_till_next_check = max(0, self.interval - time_since_last_check)
            try:
                message = self._queue.get(timeout=time_till_next_check)
            except queue.Empty:
                pass
            else:
                if message is None:
                    # None is the signal to stop this background thread
                    break
                messages.append(message)

                # Get any other messages currently on queue without blocking,
                # paying attention to None ("stop thread" signal)
                should_stop = False
                while True:
                    try:
                        message = self._queue.get_nowait()
                    except queue.Empty:
                        break
                    if message is None:
                        should_stop = True
                        break
                    messages.append(message)
                if should_stop:
                    break

            # If it's time to send, send what we've collected
            current_time = time.time()
            if current_time - last_check_time >= self.interval:
                last_check_time = current_time
                for i in range(0, len(messages), self.batch_size):
                    batch = messages[i : i + self.batch_size]
                    self.send_socket(b"".join(batch))
                messages = []

        # Send any final messages before exiting thread
        for i in range(0, len(messages), self.batch_size):
            batch = messages[i : i + self.batch_size]
            self.send_socket(b"".join(batch))
