# -*- coding: utf-8 -*-
import sys
import json
list_books = json.loads(sys.stdin.readline())

# 5 Sản phẩm giá cao nhất
list_books_sort = sorted(list_books, key=lambda x: float(x["price"]), reverse=True)
print([{"title": str(i["title"]), "price": float(i["price"]), "type_of_book": str(i["type_of_book"])} for i in list_books_sort[:5]])

# Sản phẩm còn tồn kho nhiều nhất
# list_books_sort = sorted(list_books, key=lambda x: int(x["availability"]), reverse=True)
# print([list_books_sort[:1]])


# # Sắp xếp danh sách theo giá giảm dần
# list_books_sort = sorted(list_books, key=lambda x: float(x["price"]), reverse=True)

# # Lấy sản phẩm có giá cao nhất
# highest_priced_book = list_books_sort[0]

# print({
#     "title": highest_priced_book["title"],
#     "price": float(highest_priced_book["price"]),
#     "type_of_book": highest_priced_book["type_of_book"]
# })


# Top 5 sản phẩm được đánh giá 5 sao

# # Lọc ra những sản phẩm được đánh giá 5 sao
# list_books_sort = sorted(list_books, key=lambda x: float(x["price"]), reverse=True)
# top_rated_books = list(filter(lambda x: x["rating"] == "5", list_books))

# Duyệt qua danh sách các sản phẩm được đánh giá 5 sao và in ra thông tin của từng sản phẩm
# for book in top_rated_books:
#     print("San pham duoc danh gia 5*")
#     print("Title:", book["title"])
#     print("Price:", book["price"])
#     print("Type of Book:", book["type_of_book"])
#     print()  # In một dòng trống để phân tách giữa các sản phẩm
