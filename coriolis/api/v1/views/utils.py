# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

import itertools
from typing import Iterator


def format_opt(
    option,
    keys=None,
) -> dict:
    def transform(key: str, value) -> Iterator[tuple]:
        if keys and key not in keys:
            return
        yield (key, value)

    return dict(
        itertools.chain.from_iterable(transform(k, v) for k, v in option.items())
    )
