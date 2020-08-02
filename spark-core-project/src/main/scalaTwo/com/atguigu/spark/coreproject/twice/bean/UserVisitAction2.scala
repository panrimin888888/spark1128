package com.atguigu.spark.coreproject.twice.bean

case class UserVisitAction2(date: String,
                            user_id: Long,
                            session_id: String,
                            page_id: Long,
                            action_time: String,
                            search_keyword: String,
                            click_category_id: Long,
                            click_product_id: Long,
                            order_category_ids: String,
                            order_produce_ids: String,
                            pay_category_ids: String,
                            pay_produce_ids: String,
                            city_id: Long)
