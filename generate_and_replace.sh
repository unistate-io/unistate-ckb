#!/bin/bash

# 生成实体
sea-orm-cli generate entity -o src/entity/ --model-extra-derives const_field_count::FieldCount

# 替换代码中的 Decimal 为 BigDecimal，但不替换 column_type 中的 Decimal
for file in src/entity/*.rs; do
    sed -E 's/(:?pub |Option<| )Decimal/\1BigDecimal/g' "$file" > "${file}.tmp" && mv "${file}.tmp" "$file"
done

# 格式化代码
cargo fmt --all

# 确保 const_field_count::FieldCount 被正确格式化
for file in src/entity/*.rs; do
    sed 's/const_field_count :: FieldCount/const_field_count::FieldCount/g' "$file" > "${file}.tmp" && mv "${file}.tmp" "$file"
done
