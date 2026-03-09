# class ServiceAutoRegisterMeta(type):
#    def __new__(cls, name, bases, attrs):
#        new_cls = super().__new__(cls, name, bases, attrs)
#        entity_cls = attrs.get("entity_cls")
#        #if entity_cls:
#        #    instance = new_cls()  # create instance
#        #    # service_registry.register(entity_cls, instance)
#        return new_cls
