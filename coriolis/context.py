from oslo_context import context
from oslo_db.sqlalchemy import enginefacade


@enginefacade.transaction_context_provider
class CoriolisContext(context.RequestContext):
    def __init__(self):
        self.user_id = "todo"

    def to_dict(self):
        # values = super(CoriolisContext, self).to_dict()
        values = {}
        values.update({
            'user_id': self.user_id,
        })
        return values
