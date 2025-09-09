import pytest
from allocation.adapters import repository
from allocation.service_layer import services, unit_of_work


class FakeRepository(repository.AbstractRepository):
    def __init__(self, batches):
        self._batches = set(batches)

    def add(self, batch):
        self._batches.add(batch)

    def get(self, sku):
        return next(p for p in self._batches if p.sku == sku), None

    def list(self):
        return list(self._batches)


class FakeUnitOfWork(unit_of_work.AbstractUnitOfWork):
    def __init__(self):
        self.products = FakeRepository([])
        self.committed = False

    def commit(self):
        self.committed = True

    def rollback(self):
        pass


def test_add_batch_for_new_product():
    uow = FakeUnitOfWork()
    services.add_batch("b1", "CRUNCHY-ARMCHAIR", 100, None, uow)
    assert uow.products.get("CRUNCHY-ARMCHAIR") is not None
    assert uow.committed


# def test_allocate_returns_allocation():
#     uow = FakeUnitOfWork()
#     services.add_batch("b1", "ADORABLE-SETTEE", 100, None, uow)
#     result = services.allocate("o1", "ADORABLE-SETTEE", 10, uow)
#     assert result == "batch1"
#     #assert result == "b1"


# def test_allocate_errors_for_invalid_sku():
#     uow = FakeUnitOfWork()
#     services.add_batch("b1", "AREASKU", 100, None, uow)

#     with pytest.raises(services.InvalidSku, match="Invalid sku NONEXISTENTSKU"):
#         services.allocate("o1", "NONEXISTENTSKU", 10, uow)


# def test_commits():
#     uow = FakeUnitOfWork()
#     services.add_batch("b1", "OMINOUS-MIRROR", 100, None, uow)
#     services.allocate("o1", "OMINOUS-MIRROR", 10, uow)
#     assert uow.committed
