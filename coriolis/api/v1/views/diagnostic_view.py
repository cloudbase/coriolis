# Copyright 2019 Cloudbase Solutions Srl
# All Rights Reserved.


def single(diag):
    return {"diagnostic": diag}


def collection(diag: list[dict]) -> dict[str, list]:
    return {'diagnostics': diag}
