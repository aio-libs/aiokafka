Change internal structure used for waiter in Producer.send() call. Now there should
be no performance degrade when a large backlog of messages is pending (issue #528)