"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import resource, cross_module

# cross_module 含 /api/cross 固定前缀，必须先于 resource 的
# /api/{module}/{entry_id} 动态路径注册，否则固定路径会被动态路由抢先匹配。
ROUTERS = [cross_module, resource]