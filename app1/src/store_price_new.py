import requests

to_add = {}
cached = {}
to_delete = set()

def manager(calls):
    outputs = []
    for (type, arguments) in calls:
        # invariant: items in database with product_id is the union of cached[product_id] and to_add[product_id]
        if type == "clone_product":
            product_id, new_product_id, coef, *_ = arguments
            if product_id not in cached:
                LIST_PRODUCT_URL = "http://ms1:8000/product_items"
                cached[product_id] = requests.get(url = f"{LIST_PRODUCT_URL}/{product_id}").json()

            items1 = cached[product_id]
            items2 = to_add.get(product_id, [])
            new_product_items = to_add.get(new_product_id, [])
            for items in (items1, items2):
                for item in items:
                    copy_item = dict(item)
                    copy_item.pop('id', None)
                    copy_item['product_id'] = new_product_id
                    copy_item['price'] *= coef
                    new_product_items.append(copy_item)
            to_add[new_product_id] = new_product_items
            output = len(items1)+len(items2)

        elif type == "sum_of_prices":
            product_id, *_ = arguments
            if product_id not in cached:
                LIST_PRODUCT_URL = "http://ms1:8000/product_items"
                cached[product_id] = requests.get(url = f"{LIST_PRODUCT_URL}/{product_id}").json()
            items1 = cached[product_id]
            items2 = to_add.get(product_id, [])
            sum1 = sum([i['price'] for i in items1])
            sum2 = sum([i['price'] for i in items2])
            output = round(sum1+sum2)

        else:
            # type == "delete_product"
            product_id, *_ = arguments
            cached[product_id] = []
            to_add[product_id] = []
            to_delete.add(product_id)
            output = f"Product {product_id} was successefully deleted"
        outputs.append(output)


    # update database
    LIST_PRODUCT_URL = "http://ms1:8000/product"
    for product_id in to_delete:
        requests.delete(url = f"{LIST_PRODUCT_URL}/{product_id}")

    ADD_PRODUCT_URL = "http://ms1:8000/product_item"
    for product_id in to_add:
        for item in to_add[product_id]:
            requests.put(url = ADD_PRODUCT_URL, json = item)
    return outputs