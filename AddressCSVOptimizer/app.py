from classes.class_address_optimizer import AddressOptimizer

aco = AddressOptimizer()

aco.load_file()
# aco.clean_data()
# aco.format_data()
aco.pull_db()
aco.compare_data()
# aco.push_to_db()
# aco.export_file()

print(len(aco.get_data()))
# print(len(aco.get_db_data()))
# print(len(aco._dupes))
# print(len(aco._fresh))
# print(len(aco._dupes) + len(aco._fresh))
