class Registry(dict):
    def for_queue(self, queues=None, excluded_queues=None):
        for item in self.values():
            if queues and item.queue not in queues:
                continue
            if excluded_queues and item.queue in excluded_queues:
                continue
            yield item


schedules_registry = Registry()
tasks_registry = Registry()
