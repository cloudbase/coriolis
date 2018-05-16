# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_context import context
from oslo_db.sqlalchemy import enginefacade
from oslo_utils import timeutils


@enginefacade.transaction_context_provider
class RequestContext(context.RequestContext):
    def __init__(self, user, tenant, is_admin=None,
                 roles=None, project_name=None, remote_address=None,
                 timestamp=None, request_id=None, auth_token=None,
                 overwrite=True, domain_name=None, domain_id=None,
                 user_domain_name=None, user_domain_id=None,
                 project_domain_name=None, project_domain_id=None,
                 show_deleted=None, trust_id=None,
                 delete_trust_id=False, **kwargs):

        super(RequestContext, self).__init__(auth_token=auth_token,
                                             user=user,
                                             tenant=tenant,
                                             domain_name=domain_name,
                                             domain_id=domain_id,
                                             user_domain_name=user_domain_name,
                                             user_domain_id=user_domain_id,
                                             project_domain_name=(
                                                 project_domain_name),
                                             project_domain_id=(
                                                 project_domain_id),
                                             is_admin=is_admin,
                                             show_deleted=show_deleted,
                                             request_id=request_id,
                                             overwrite=overwrite)
        self.roles = roles or []
        self.project_name = project_name
        self.remote_address = remote_address
        if not timestamp:
            timestamp = timeutils.utcnow()
        elif isinstance(timestamp, str):
            timestamp = timeutils.parse_isotime(timestamp)
        self.timestamp = timestamp
        self.trust_id = trust_id
        self.delete_trust_id = delete_trust_id

    def to_dict(self):
        result = super(RequestContext, self).to_dict()
        result['user'] = self.user
        result['tenant'] = self.tenant
        result['project_name'] = self.project_name
        result['domain_id'] = self.domain_id
        result['domain_name'] = self.domain_name
        result['user_domain_id'] = self.user_domain_id
        result['user_domain_name'] = self.user_domain_name
        result['project_domain_id'] = self.project_domain_id
        result['project_domain_name'] = self.project_domain_name
        result['roles'] = self.roles
        result['remote_address'] = self.remote_address
        result['timestamp'] = self.timestamp.isoformat()
        result['request_id'] = self.request_id
        result['show_deleted'] = self.show_deleted
        result['trust_id'] = self.trust_id
        return result

    @classmethod
    def from_dict(cls, values):
        return cls(**values)


def get_admin_context(trust_id=None):
    return RequestContext(
        user=None, tenant=None, is_admin=True,
        trust_id=trust_id)
