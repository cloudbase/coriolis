# Copyright 2019 Cloudbase Solutions Srl
# All Rights Reserved.


def single(req, diag):
    return {"diagnostic":  diag}


def collection(req, diag):
    return {'diagnostics': diag}
