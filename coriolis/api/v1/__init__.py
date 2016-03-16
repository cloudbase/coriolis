import paste.urlmap


def root_app_factory(loader, global_conf, **local_conf):
    return paste.urlmap.urlmap_factory(loader, global_conf, **local_conf)
