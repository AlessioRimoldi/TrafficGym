try:
    import django_stubs_ext
    django_stubs_ext.monkeypatch()
except ImportError:
    pass
