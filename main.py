import asyncio
import time
from typing import Dict, Any, List, Tuple, Optional, Set
import httpx
import json 
import datetime 
import re
import cn2an
from natsort import natsorted
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, register
from astrbot.api import logger

class InstanceCooldownManager:
    """实例操作冷却时间管理"""
    def __init__(self):
        self.cooldowns: Dict[str, float] = {}

    def check_cooldown(self, instance_id: str) -> bool:
        """检查实例是否在冷却中（10秒冷却）"""
        last_time = self.cooldowns.get(instance_id, 0)
        return time.time() - last_time < 10

    def set_cooldown(self, instance_id: str):
        """设置实例冷却时间"""
        self.cooldowns[instance_id] = time.time()

def format_uptime_seconds(seconds: float) -> str:
    """将秒数转换为 天/小时/分钟 的可读格式"""
    if seconds is None or seconds <= 0:
        return "未知"
    seconds = int(seconds)
    # 1. 转换为分钟和剩余秒数
    minutes, seconds = divmod(seconds, 60)
    # 2. 转换为小时和剩余分钟
    hours, minutes = divmod(minutes, 60)
    # 3. 转换为天和剩余小时
    days, hours = divmod(hours, 24)

    parts = []
    if days > 0:
        parts.append(f"{days}天")
    if hours > 0:
        parts.append(f"{hours}小时")
    if minutes > 0:
        parts.append(f"{minutes}分钟")
    
    # 如果不足一分钟，则显示秒
    if not parts:
        return f"{seconds}秒"
    
    # 限制只显示最长的两个单位，避免结果太长
    return "".join(parts[:2]) if len(parts) > 1 else "".join(parts)


@register("MCSManager", "5060的3600马力、Kx501", "MCSManager服务器管理插件", "2.0.25.15") 
class MCSMPlugin(Star):
    def __init__(self, context: Context, config: dict):
        super().__init__(context)
        self.config = config
        self.cooldown_manager = InstanceCooldownManager()
        self.http_client = httpx.AsyncClient(timeout=30.0)
        # 批量操作间隔时间（秒）
        self.batch_interval = float(self.config.get("batch_operation_interval", 2.0))
        # 缓存实例数据，用于名称/编号/UUID查找
        self.instance_data: Dict[str, Any] = {
            "instances": [], # 实例列表 [{'index': str, 'name': str, 'daemon_id': str, 'uuid': str, 'status': int}, ...]
            "name_to_id": {}, # 仅存储唯一名称 -> (daemon_id, uuid) 映射
            "uuid_to_id": {}, # UUID -> (daemon_id, uuid) 映射
            "ambiguous_names": set(), # 存储所有重名实例的名称
        }
        # 创建后台任务自动刷新缓存（只执行一次）
        asyncio.create_task(self._refresh_instance_cache_async())
        logger.info("MCSM插件(v10)初始化完成喵~出现问题及时提issue！")

    async def terminate(self):
        """插件卸载时关闭HTTP客户端"""
        await self.http_client.aclose()
        logger.info("MCSM插件已卸载")

    def _extract_user_id(self, raw_id: str) -> str:
        """
        从 CQ 码、自定义 At 格式或纯字符串中提取用户 ID
        """
        raw_id = raw_id.strip()
        
        # 1. 匹配标准 QQ-CQ 码格式: [CQ:at,qq=ID]
        match = re.search(r'\[CQ:at,qq=(\d+)\]', raw_id)
        if match:
            return match.group(1)

        # 2. 匹配 AstrBot 自定义 At 格式: [At:ID]
        match = re.search(r'\[At:(\d+)\]', raw_id)
        if match:
            return match.group(1)

        # 3. 匹配 QQ/群聊 @ 格式: @Name(ID) 或其他包含 ID 在括号内的格式
        match = re.search(r'\((\d+)\)', raw_id)
        if match:
            return match.group(1)
        
        # 4. 如果是纯数字 ID
        if raw_id.isdigit():
            return raw_id
            
        # 否则原样返回
        return raw_id

    def _get_sort_key(self, text: str) -> Tuple[int, str]:
        """
        生成排序键，用于分开排序阿拉伯数字和中文数字
        返回: (是否包含中文数字, 转换后的字符串)
        0 = 无中文数字（排在前面）
        1 = 有中文数字（排在后面）
        """
        if not text:
            return (0, text)
        
        # 匹配中文数字的正则表达式
        chinese_number_pattern = r'[零一二三四五六七八九十百千万]+'
        
        # 检查是否包含中文数字
        has_chinese_number = bool(re.search(chinese_number_pattern, text))
        
        # 转换中文数字为阿拉伯数字
        def replace_chinese_number(match):
            chinese_num = match.group(0)
            try:
                arabic_num = cn2an.cn2an(chinese_num, "normal")
                return str(arabic_num)
            except (ValueError, KeyError):
                return chinese_num
        
        converted_text = re.sub(chinese_number_pattern, replace_chinese_number, text)
        
        # 返回 (是否包含中文数字, 转换后的字符串)
        return (1 if has_chinese_number else 0, converted_text)

    async def make_mcsm_request(self, endpoint: str, method: str = "GET", params: dict = None, data: dict = None) -> dict:
        """发送请求到MCSManager API"""
        base_url = self.config['mcsm_url'].rstrip('/')
        
        if not endpoint.startswith('/api/'):
            url = f"{base_url}/api{endpoint}"
        else:
            url = f"{base_url}{endpoint}"
        
        query_params = {"apikey": self.config["api_key"]}
        if params:
            query_params.update(params)

        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "X-Requested-With": "XMLHttpRequest"
        }

        try:
            if method.upper() == "GET":
                response = await self.http_client.get(url, params=query_params, headers=headers)
            elif method.upper() == "POST":
                response = await self.http_client.post(url, params=query_params, json=data, headers=headers)
            elif method.upper() == "PUT":
                response = await self.http_client.put(url, params=query_params, json=data, headers=headers)
            elif method.upper() == "DELETE":
                response = await self.http_client.delete(url, params=query_params, json=data, headers=headers)
            else:
                return {"status": 400, "error": "不支持的请求方法"}

            if response.status_code != 200:
                try:
                    # 尝试解析错误信息
                    return response.json()
                except:
                    # 如果不是JSON，返回文本信息
                    return {"status": response.status_code, "error": f"HTTP Error {response.status_code}: {response.text[:100]}..."}

            try:
                return response.json()
            except Exception as json_e:
                return {"status": 500, "error": f"JSON解析失败: {str(json_e)}"}

        except httpx.ConnectTimeout as e:
            return {"status": 504, "error": "连接超时 (ConnectTimeout)"}
        except httpx.ReadTimeout as e:
            return {"status": 504, "error": "读取超时 (ReadTimeout)"}
        except Exception as e:
            logger.error(f"MCSM API请求失败: {str(e)}")
            return {"status": 500, "error": str(e)}

    def _command_requires_authorized(self, subcommand: str) -> bool:
        """判断该子指令是否要求授权用户身份（以配置 authorized_only_commands 为准）"""
        commands = self.config.get("authorized_only_commands", [])
        return subcommand in commands

    def _check_authorized_for_command(self, event: AstrMessageEvent, subcommand: str) -> bool:
        """该子指令允许当前用户执行则返回 True，否则 False"""
        if not self._command_requires_authorized(subcommand):
            return True
        return self.is_admin_or_authorized(event)

    def is_admin_or_authorized(self, event: AstrMessageEvent) -> bool:
        """检查是否为授权用户（仅依据本插件配置的授权用户/群组，不区分机器人管理员）"""
        authorized_groups = self.config.get("authorized_groups", [])
        authorized_users = self.config.get("authorized_users", [])

        if not authorized_groups and not authorized_users:
            return True

        if authorized_groups:
            group_id = event.message_obj.group_id if hasattr(event, 'message_obj') and hasattr(event.message_obj, 'group_id') else ""
            if group_id and group_id in authorized_groups:
                return True

        if authorized_users:
            user_id = str(event.get_sender_id())
            if user_id in authorized_users:
                return True

        return False

    def _should_filter_instance(self, instance_name: str) -> bool:
        """
        检查实例名称是否应该被过滤。
        如果实例名称不包含配置中任意关键词，返回 True（应该过滤）。
        如果包含任意关键词，返回 False（应该保留，白名单模式）。
        """
        filtered_keywords = self.config.get("filtered_instance_keywords", [])
        if not filtered_keywords:
            return False
        
        instance_name_lower = instance_name.lower()
        for keyword in filtered_keywords:
            if keyword and keyword.lower() in instance_name_lower:
                return False  # 包含关键词，应该保留
        return True  # 不包含任何关键词，应该过滤

    def _is_uuid_format(self, identifier: str) -> bool:
        """判断是否为UUID格式（32位十六进制，可能包含连字符）"""
        # 去除连字符
        cleaned = identifier.replace('-', '')
        # 检查长度和字符集
        return len(cleaned) == 32 and all(c in '0123456789abcdefABCDEF' for c in cleaned)

    def _detect_identifier_type(self, identifier: str) -> str:
        """检测标识符类型：'number', 'uuid', 'name'"""
        if identifier.isdigit():
            return 'number'
        if self._is_uuid_format(identifier):
            return 'uuid'
        return 'name'

    async def _refresh_instance_cache_async(self) -> bool:
        """
        自动刷新实例缓存，不显示结果给用户
        返回True表示成功，False表示失败
        """
        try:
            overview_resp = await self.make_mcsm_request("/overview")
            
            nodes: List[Dict[str, Any]] = []
            if overview_resp.get("status") == 200:
                nodes = overview_resp.get("data", {}).get("remote", [])
                # 按节点名称进行自然排序（支持中文数字，分开排序）
                nodes = natsorted(nodes, key=lambda x: self._get_sort_key(
                    x.get("remarks") or x.get("ip") or "Unnamed Node"
                ))
            
            if not nodes:
                logger.warning("自动刷新缓存失败: 无法从 /overview 获取节点信息")
                return False

            # 按节点分组存储实例
            instances_by_node: Dict[str, List[Dict[str, Any]]] = {}
            
            # 获取要排除的节点列表
            filtered_nodes = self.config.get("filtered_nodes", [])

            # 1. 收集所有实例，按节点分组
            for node in nodes:
                node_uuid = node.get("uuid")
                # 如果节点在排除列表中，跳过该节点
                if node_uuid in filtered_nodes:
                    continue
                
                instances_by_node[node_uuid] = []

                # 兼容 v10 API，查询指定节点下的实例
                instances_resp = await self.make_mcsm_request(
                    "/service/remote_service_instances",
                    params={"daemonId": node_uuid, "page": 1, "page_size": 100}
                )

                if instances_resp.get("status") != 200:
                    continue

                data_block = instances_resp.get("data", {})
                # 兼容 API 返回数据结构不一致的情况
                instances = data_block.get("data", []) if isinstance(data_block, dict) else data_block
                
                for instance in instances:
                    inst_name = instance.get("config", {}).get("nickname") or "未命名"
                    # 检查是否应该过滤该实例
                    if self._should_filter_instance(inst_name):
                        continue
                    
                    inst_uuid = instance.get("instanceUuid")
                    status_code = instance.get("status")
                    if status_code is None and "info" in instance:
                        status_code = instance["info"].get("status")
                    
                    instances_by_node[node_uuid].append({
                        "name": inst_name,
                        "uuid": inst_uuid,
                        "daemon_id": node_uuid,
                        "status": status_code,
                    })
            
            # 2. 收集所有实例用于重名检测（跨节点检测）
            all_instances: List[Dict[str, Any]] = []
            for node_uuid, instances in instances_by_node.items():
                all_instances.extend(instances)
            
            # 3. 预处理: 找出重名实例
            name_counts: Dict[str, int] = {}
            for instance in all_instances:
                name = instance['name']
                name_counts[name] = name_counts.get(name, 0) + 1

            ambiguous_names: Set[str] = {name for name, count in name_counts.items() if count > 1}

            # 4. 构建缓存（不生成显示文本）
            self.instance_data["instances"] = []
            self.instance_data["name_to_id"] = {}
            self.instance_data["uuid_to_id"] = {}
            self.instance_data["ambiguous_names"] = ambiguous_names
            
            current_index = 1

            # 按节点遍历构建缓存
            for node_uuid, instances in instances_by_node.items():
                if not instances:
                    continue
                
                # 节点内按名称自然排序（支持中文数字，分开排序）
                instances[:] = natsorted(instances, key=lambda x: self._get_sort_key(x['name']))
                
                # 构建缓存数据
                for instance in instances:
                    inst_name = instance['name']
                    inst_uuid = instance['uuid']
                    is_ambiguous = inst_name in ambiguous_names
                    
                    instance_data = {
                        "index": str(current_index),
                        "name": inst_name,
                        "uuid": inst_uuid,
                        "daemon_id": node_uuid,
                        "status": instance['status']
                    }
                    
                    self.instance_data["instances"].append(instance_data)
                    self.instance_data["uuid_to_id"][inst_uuid] = (node_uuid, inst_uuid)
                    
                    # 只有唯一名称才加入 name_to_id，重名名称不加入
                    if not is_ambiguous:
                        self.instance_data["name_to_id"][inst_name] = (node_uuid, inst_uuid)
                    
                    current_index += 1
            
            logger.info(f"MCSM插件: 自动刷新缓存完成，共 {len(all_instances)} 个实例")
            return True
        except Exception as e:
            logger.error(f"MCSM插件: 自动刷新缓存失败: {str(e)}")
            return False

    def _get_instance_by_identifier(self, identifier: str) -> Optional[Tuple[str, str]]:
        """
        通过实例名、索引或 UUID 查找对应的 (daemonId, instanceUuid)。
        查找优先级：纯数字=编号，32位十六进制=UUID，其他=名称
        """
        identifier = identifier.strip()
        
        # 1. 纯数字 → 作为编号处理
        if identifier.isdigit():
            index = int(identifier)
            instances = self.instance_data.get("instances", [])
            # 索引是 1-based, 列表是 0-based
            if 0 < index <= len(instances):
                instance_data = instances[index - 1]
                # 检查是否应该过滤该实例
                if self._should_filter_instance(instance_data['name']):
                    return None
                return instance_data['daemon_id'], instance_data['uuid']
            # 超出范围，返回None（不再尝试作为名称）
            return None
        
        # 2. 32位十六进制字符串 → 作为 UUID 查找
        if self._is_uuid_format(identifier):
            if identifier in self.instance_data["uuid_to_id"]:
                daemon_id, instance_uuid = self.instance_data["uuid_to_id"][identifier]
                # 从缓存中查找实例名称
                for inst_data in self.instance_data.get("instances", []):
                    if inst_data['uuid'] == instance_uuid:
                        if self._should_filter_instance(inst_data['name']):
                            return None
                        break
                return daemon_id, instance_uuid
            # UUID格式但找不到，返回None
            return None
        
        # 3. 其他字符串 → 作为名称查找
        # 检查是否是重名实例，如果是，则不允许通过名称操作
        if identifier in self.instance_data.get("ambiguous_names", set()):
            logger.warning(f"用户尝试通过重名实例名称操作: {identifier}。已拒绝。")
            return None

        if identifier in self.instance_data["name_to_id"]:
            # 检查是否应该过滤该实例
            instance_name = identifier
            if self._should_filter_instance(instance_name):
                return None
            return self.instance_data["name_to_id"][identifier]

        return None

    def _collect_instances_for_batch(
        self,
        identifiers: List[str]
    ) -> Tuple[Optional[List[Tuple[str, str, str, str]]], Optional[List[str]]]:
        """
        收集批量操作的实例
        返回：(成功收集的实例列表, 失败的标识符列表) 或 (None, None) 表示类型不一致
        实例格式：(ident, daemon_id, instance_id, instance_name)
        """
        # 过滤空字符串
        identifiers = [ident.strip() for ident in identifiers if ident.strip()]
        if not identifiers:
            return [], []
        
        # 统一类型检查
        first_type = self._detect_identifier_type(identifiers[0])
        for ident in identifiers:
            if self._detect_identifier_type(ident) != first_type:
                return None, None  # 类型不一致，返回特殊值
        
        # 收集实例
        instances = []
        failed_identifiers = []
        
        for ident in identifiers:
            ids = self._get_instance_by_identifier(ident)
            if ids:
                daemon_id, instance_id = ids
                # 获取实例名称
                instance_name = ident
                for data in self.instance_data.get("instances", []):
                    if data['uuid'] == instance_id:
                        instance_name = data['name']
                        break
                instances.append((ident, daemon_id, instance_id, instance_name))
            else:
                failed_identifiers.append(ident)
        
        return instances, failed_identifiers

    async def _process_single_instance(
        self,
        event: AstrMessageEvent,
        identifier: str,
        operation_emoji: str,  # "🚀" 或 "🛑"
        operation_name: str,  # "启动" 或 "停止"
        api_endpoint: str  # "/protected_instance/open" 或 "/protected_instance/stop"
    ):
        """单实例操作的通用处理逻辑"""
        ids = self._get_instance_by_identifier(identifier)
        if not ids:
            if identifier in self.instance_data.get("ambiguous_names", set()):
                yield event.plain_result(f"❌ {operation_name}失败: 实例名称 '{identifier}' 重复。请使用 编号/UUID 进行操作。")
            else:
                yield event.plain_result(f"❌ 找不到实例: {identifier}。请确认名称/编号或/UUID正确，并先运行 /mcsm list 更新列表。")
            return
        
        daemon_id, instance_id = ids
        
        if self.cooldown_manager.check_cooldown(instance_id):
            yield event.plain_result("⏳ 操作太快了，请稍后再试")
            return
        
        # 获取实例名称
        instance_name = identifier
        for data in self.instance_data.get("instances", []):
            if data['uuid'] == instance_id:
                instance_name = data['name']
                break
        
        yield event.plain_result(f"{operation_emoji} 正在{operation_name}: {instance_name} ...")
        
        resp = await self.make_mcsm_request(
            api_endpoint,
            method="GET",
            params={"uuid": instance_id, "daemonId": daemon_id}
        )
        
        if resp.get("status") != 200:
            err = resp.get("data") or resp.get("error") or "未知错误"
            status_code = resp.get("status", "???")
            yield event.plain_result(f"❌ {operation_name}失败: [{status_code}] {err}")
            return
        
        self.cooldown_manager.set_cooldown(instance_id)
        yield event.plain_result(f"✅ {instance_name} {operation_name}命令已发送")

    @filter.command("mcsm help")
    async def mcsm_main(self, event: AstrMessageEvent):
        """显示帮助信息"""
        if not self._check_authorized_for_command(event, "help"):
            yield event.plain_result("❌ 权限不足")
            return
            
        help_text = """
🛠️ MCSM面板 管理指令：
/mcsm help - 显示此帮助
/mcsm status - 面板状态概览
/mcsm list - 节点实例列表 (按名称排序，提供编号)
/mcsm op <qq/@> - 授权用户插件管理员身份
/mcsm deop <qq/@> - 取消用户插件管理员身份

> 实例操作 (支持 名称/编号/UUID) ---
/mcsm start <实例1> [实例2] - 批量启动
/mcsm stop <实例1> [实例2] - 批量停止
/mcsm restart <实例1> [实例2] - 批量重启
/mcsm kill <实例1> [实例2] - 批量终止
/mcsm cmd <实例> [命令] - 发送命令
/mcsm log <实例> - 查看最近日志
"""
        yield event.plain_result(help_text)

    @filter.command("mcsm op")
    async def mcsm_auth(self, event: AstrMessageEvent, user_id: str):
        """授权用户"""
        if not self._check_authorized_for_command(event, "op"):
            yield event.plain_result("❌ 权限不足")
            return
        user_id = self._extract_user_id(user_id) 
        
        if not user_id.isdigit():
            yield event.plain_result(f"❌ 授权失败: 请提供有效的用户ID或正确的 @提及格式，当前输入: {user_id}")
            return

        authorized_users = self.config.get("authorized_users", [])
        if user_id in authorized_users:
            yield event.plain_result(f"用户 {user_id} 已在授权列表中")
            return

        authorized_users.append(user_id)
        self.config["authorized_users"] = authorized_users
        
        try:
            self.context.save_config()
            yield event.plain_result(f"✅ 已授权用户 {user_id}")
        except AttributeError:
             yield event.plain_result(f"✅ 授权成功！用户 {user_id} 已添加到配置 ")
        except Exception as e:
             yield event.plain_result(f"❌ 授权失败 (保存配置异常): {str(e)}")

    @filter.command("mcsm deop")
    async def mcsm_unauth(self, event: AstrMessageEvent, user_id: str):
        """取消用户授权"""
        if not self._check_authorized_for_command(event, "deop"):
            yield event.plain_result("❌ 权限不足")
            return
        user_id = self._extract_user_id(user_id)

        if not user_id.isdigit():
            yield event.plain_result(f"❌ 取消授权失败: 请提供有效的用户ID或正确的 @提及格式，当前输入: {user_id}")
            return

        authorized_users = self.config.get("authorized_users", [])
        if user_id not in authorized_users:
            yield event.plain_result(f"用户 {user_id} 未获得授权")
            return

        authorized_users.remove(user_id)
        self.config["authorized_users"] = authorized_users
        
        try:
            self.context.save_config()
            yield event.plain_result(f"✅ 已取消用户 {user_id} 的授权")
        except AttributeError:
             yield event.plain_result(f"✅ 用户 {user_id} 已从配置移除。")
        except Exception as e:
             yield event.plain_result(f"❌ 取消授权失败 (保存配置异常): {str(e)}")

    @filter.command("mcsm list")
    async def mcsm_list(self, event: AstrMessageEvent):
        """查看实例列表"""
        if not self._check_authorized_for_command(event, "list"):
            yield event.plain_result("❌ 权限不足")
            return
        yield event.plain_result("正在获取节点和实例数据，请稍候...")

        overview_resp = await self.make_mcsm_request("/overview")
        
        nodes: List[Dict[str, Any]] = []
        if overview_resp.get("status") == 200:
            nodes = overview_resp.get("data", {}).get("remote", [])
            # 按节点名称进行自然排序（支持中文数字，分开排序）
            nodes = natsorted(nodes, key=lambda x: self._get_sort_key(
                x.get("remarks") or x.get("ip") or "Unnamed Node"
            ))
        
        if not nodes:
            yield event.plain_result(
                f"⚠️ 无法从 /overview 获取节点信息。API 响应: {overview_resp.get('error', '未知错误')}"
            )
            return

        # 按节点分组存储实例
        instances_by_node: Dict[str, List[Dict[str, Any]]] = {}
        node_details: Dict[str, Dict[str, str]] = {} # To store node info for the final list

        # 获取要排除的节点列表
        filtered_nodes = self.config.get("filtered_nodes", [])

        # 1. 收集所有实例，按节点分组
        for node in nodes:
            node_uuid = node.get("uuid")
            # 如果节点在排除列表中，跳过该节点
            if node_uuid in filtered_nodes:
                continue
            node_name = node.get("remarks") or node.get("ip") or "Unnamed Node"
            
            node_details[node_uuid] = {"name": node_name}
            instances_by_node[node_uuid] = []

            # 兼容 v10 API，查询指定节点下的实例
            instances_resp = await self.make_mcsm_request(
                "/service/remote_service_instances",
                params={"daemonId": node_uuid, "page": 1, "page_size": 100}
            )

            if instances_resp.get("status") != 200:
                # Log error but continue to next node
                continue

            data_block = instances_resp.get("data", {})
            # 兼容 API 返回数据结构不一致的情况
            instances = data_block.get("data", []) if isinstance(data_block, dict) else data_block
            
            for instance in instances:
                inst_name = instance.get("config", {}).get("nickname") or "未命名"
                # 检查是否应该过滤该实例
                if self._should_filter_instance(inst_name):
                    continue
                
                inst_uuid = instance.get("instanceUuid")
                status_code = instance.get("status")
                if status_code is None and "info" in instance:
                    status_code = instance["info"].get("status")
                
                instances_by_node[node_uuid].append({
                    "name": inst_name,
                    "uuid": inst_uuid,
                    "daemon_id": node_uuid,
                    "status": status_code,
                })
        
        # 2. 收集所有实例用于重名检测（跨节点检测）
        all_instances: List[Dict[str, Any]] = []
        for node_uuid, instances in instances_by_node.items():
            all_instances.extend(instances)
        
        # 3. 预处理: 找出重名实例
        name_counts: Dict[str, int] = {}
        for instance in all_instances:
            name = instance['name']
            name_counts[name] = name_counts.get(name, 0) + 1

        ambiguous_names: Set[str] = {name for name, count in name_counts.items() if count > 1}

        # 4. 构建缓存和输出结果
        self.instance_data["instances"] = []
        self.instance_data["name_to_id"] = {} # 仅存储唯一名称的映射
        self.instance_data["uuid_to_id"] = {}
        self.instance_data["ambiguous_names"] = ambiguous_names # 存储重名集合
        
        result = "🖥️ MCSM 实例列表:\n"
        
        current_index = 1

        # 获取是否显示UUID的配置
        show_uuid = self.config.get("show_uuid", True)

        # v10 状态码: -1:未知, 0:停止, 1:停止中, 2:启动中, 3:运行中
        # status_map = {3: "🟢", 0: "🔴", 1: "🟠", 2: "🟡", -1: "⚪"}
        status_map = {3: "✔", 0: "✘", 1: "⚑", 2: "⛟", -1: "☠"}

        # 按节点遍历显示
        for node_uuid, instances in instances_by_node.items():
            if not instances:
                continue
            
            # 显示节点信息
            node_name = node_details.get(node_uuid, {}).get("name", "未知节点")
            result += f"\n⛽ 节点: {node_name}\n"
            result += f"Daemon ID: {node_uuid}\n"
            
            # 节点内按名称自然排序（支持中文数字，分开排序）
            instances[:] = natsorted(instances, key=lambda x: self._get_sort_key(x['name']))
            
            # 显示该节点下的所有实例
            for instance in instances:
                inst_name = instance['name']
                inst_uuid = instance['uuid']
                status_icon = status_map.get(instance['status'], "☠")
                is_ambiguous = inst_name in ambiguous_names # 检查是否重名
                
                # 打印实例信息：状态图标 + 编号 + 实例名称
                ambiguity_tag = " (☢重名)" if is_ambiguous else "" # 添加重名标记
                result += f"{status_icon} [{current_index}] {inst_name}{ambiguity_tag}\n"
                # UUID单独一行显示，用缩进表示层级（根据配置决定是否显示）
                if show_uuid:
                    result += f"- {inst_uuid}\n"
                
                # 构建缓存数据
                instance_data = {
                    "index": str(current_index),
                    "name": inst_name,
                    "uuid": inst_uuid,
                    "daemon_id": node_uuid,
                    "status": instance['status']
                }
                
                self.instance_data["instances"].append(instance_data)
                self.instance_data["uuid_to_id"][inst_uuid] = (node_uuid, inst_uuid)
                
                # 只有唯一名称才加入 name_to_id，重名名称不加入喵
                if not is_ambiguous:
                    self.instance_data["name_to_id"][inst_name] = (node_uuid, inst_uuid)
                
                current_index += 1
        
        if not all_instances:
             result += "\n(此面板下暂无实例)\n"
             
        result += "\n💡 提示: 使用 /mcsm start [名称/编号] 即可操作。"
        if ambiguous_names:
            result += "\n\n☢ 注意: 标记 '☢重名' 的实例，请使用编号/UUID 进行操作。"


        yield event.plain_result(result)

    @filter.command("mcsm start")
    async def mcsm_start(self, event: AstrMessageEvent, identifier: str):
        """启动实例 (支持名称/编号/UUID，支持批量操作)"""
        if not self._check_authorized_for_command(event, "start"):
            yield event.plain_result("❌ 权限不足")
            return
        # 从完整消息中提取所有标识符
        raw_msg = event.message_str.strip()
        parts = raw_msg.split(maxsplit=2)  # 分割为: ["/mcsm", "start", "2 3"]
        
        if len(parts) < 3:
            # 没有提供标识符，使用 identifier 参数（向后兼容）
            identifiers = [identifier.strip()] if identifier.strip() else []
        else:
            # 提取所有标识符（支持空格分隔的多个标识符）
            identifiers = [ident.strip() for ident in parts[2].strip().split() if ident.strip()]
        
        # 批量操作
        if len(identifiers) > 1:
            instances, failed_identifiers = self._collect_instances_for_batch(identifiers)
            
            if instances is None:  # 类型不一致
                yield event.plain_result(f"❌ 批量操作时所有标识符必须是同一类型（编号/UUID/名称），当前混合使用了不同类型")
                return
            
            if not instances:
                yield event.plain_result(f"❌ 批量启动失败: 所有标识符都找不到对应的实例")
                return
            
            # 发送开始消息
            yield event.plain_result(f"🚀 开始批量启动 {len(instances)} 个实例...")
            await asyncio.sleep(self.batch_interval)
            
            # 收集所有操作结果，循环中不 yield
            success_count = 0
            fail_count = 0
            fail_details = []
            result_messages = []  # 收集所有结果消息
            
            for idx, (ident, daemon_id, instance_id, instance_name) in enumerate(instances, 1):
                # 检查冷却
                if self.cooldown_manager.check_cooldown(instance_id):
                    result_messages.append(f"⏳ {instance_name} 操作太快了，跳过")
                    fail_count += 1
                    fail_details.append(f"{instance_name}: 操作太快")
                    await asyncio.sleep(self.batch_interval)  # 保持延迟，但不 yield
                    continue
                
                # 执行 API 请求
                resp = await self.make_mcsm_request(
                    "/protected_instance/open",
                    method="GET",
                    params={"uuid": instance_id, "daemonId": daemon_id}
                )
                
                if resp.get("status") != 200:
                    err = resp.get("data") or resp.get("error") or "未知错误"
                    status_code = resp.get("status", "???")
                    result_messages.append(f"❌ {instance_name} 启动失败: [{status_code}] {err}")
                    fail_count += 1
                    fail_details.append(f"{instance_name}: {err}")
                else:
                    self.cooldown_manager.set_cooldown(instance_id)
                    result_messages.append(f"✅ {instance_name} 启动命令已发送")
                    success_count += 1
                
                # 每个实例处理完后延迟（除了最后一个）
                if idx < len(instances):
                    await asyncio.sleep(self.batch_interval)
            
            # 循环结束后，一次性发送所有结果
            # 构建完整的结果消息
            result_msg = f"📊 批量启动完成: 成功 {success_count} 个，失败 {fail_count} 个\n\n"
            result_msg += "\n".join(result_messages)
            
            if failed_identifiers:
                result_msg += f"\n\n⚠️ 未找到的标识符: {', '.join(failed_identifiers)}"
            if fail_details:
                result_msg += f"\n\n❌ 失败详情:\n" + "\n".join(fail_details)
            
            yield event.plain_result(result_msg)
            return
        
        # 单实例操作
        if not identifiers:
            yield event.plain_result("❌ 请输入有效的实例标识符")
            return
        
        # 使用第一个标识符（单实例操作）
        async for result in self._process_single_instance(
            event, identifiers[0], "🚀", "启动", "/protected_instance/open"
        ):
            yield result

    @filter.command("mcsm stop")
    async def mcsm_stop(self, event: AstrMessageEvent, identifier: str):
        """停止实例 (支持名称/编号/UUID，支持批量操作)"""
        if not self._check_authorized_for_command(event, "stop"):
            yield event.plain_result("❌ 权限不足")
            return
        # 从完整消息中提取所有标识符
        raw_msg = event.message_str.strip()
        parts = raw_msg.split(maxsplit=2)  # 分割为: ["/mcsm", "stop", "2 3"]
        
        if len(parts) < 3:
            # 没有提供标识符，使用 identifier 参数（向后兼容）
            identifiers = [identifier.strip()] if identifier.strip() else []
        else:
            # 提取所有标识符（支持空格分隔的多个标识符）
            identifiers = [ident.strip() for ident in parts[2].strip().split() if ident.strip()]
        
        # 批量操作
        if len(identifiers) > 1:
            instances, failed_identifiers = self._collect_instances_for_batch(identifiers)
            
            if instances is None:  # 类型不一致
                yield event.plain_result(f"❌ 批量操作时所有标识符必须是同一类型（编号/UUID/名称），当前混合使用了不同类型")
                return
            
            if not instances:
                yield event.plain_result(f"❌ 批量停止失败: 所有标识符都找不到对应的实例")
                return
            
            # 发送开始消息
            yield event.plain_result(f"🛑 开始批量停止 {len(instances)} 个实例...")
            await asyncio.sleep(self.batch_interval)
            
            # 收集所有操作结果，循环中不 yield
            success_count = 0
            fail_count = 0
            fail_details = []
            result_messages = []  # 收集所有结果消息
            
            for idx, (ident, daemon_id, instance_id, instance_name) in enumerate(instances, 1):
                # 检查冷却
                if self.cooldown_manager.check_cooldown(instance_id):
                    result_messages.append(f"⏳ {instance_name} 操作太快了，跳过")
                    fail_count += 1
                    fail_details.append(f"{instance_name}: 操作太快")
                    await asyncio.sleep(self.batch_interval)  # 保持延迟，但不 yield
                    continue
                
                # 执行 API 请求
                resp = await self.make_mcsm_request(
                    "/protected_instance/stop",
                    method="GET",
                    params={"uuid": instance_id, "daemonId": daemon_id}
                )
                
                if resp.get("status") != 200:
                    err = resp.get("data") or resp.get("error") or "未知错误"
                    status_code = resp.get("status", "???")
                    result_messages.append(f"❌ {instance_name} 停止失败: [{status_code}] {err}")
                    fail_count += 1
                    fail_details.append(f"{instance_name}: {err}")
                else:
                    self.cooldown_manager.set_cooldown(instance_id)
                    result_messages.append(f"✅ {instance_name} 停止命令已发送")
                    success_count += 1
                
                # 每个实例处理完后延迟（除了最后一个）
                if idx < len(instances):
                    await asyncio.sleep(self.batch_interval)
            
            # 循环结束后，一次性发送所有结果
            # 构建完整的结果消息
            result_msg = f"📊 批量停止完成: 成功 {success_count} 个，失败 {fail_count} 个\n\n"
            result_msg += "\n".join(result_messages)
            
            if failed_identifiers:
                result_msg += f"\n\n⚠️ 未找到的标识符: {', '.join(failed_identifiers)}"
            if fail_details:
                result_msg += f"\n\n❌ 失败详情:\n" + "\n".join(fail_details)
            
            yield event.plain_result(result_msg)
            return
        
        # 单实例操作
        if not identifiers:
            yield event.plain_result("❌ 请输入有效的实例标识符")
            return
        
        # 使用第一个标识符（单实例操作）
        async for result in self._process_single_instance(
            event, identifiers[0], "🛑", "停止", "/protected_instance/stop"
        ):
            yield result

    @filter.command("mcsm restart")
    async def mcsm_restart(self, event: AstrMessageEvent, identifier: str):
        """重启实例 (支持名称/编号/UUID，支持批量操作)"""
        if not self._check_authorized_for_command(event, "restart"):
            yield event.plain_result("❌ 权限不足")
            return
        # 从完整消息中提取所有标识符
        raw_msg = event.message_str.strip()
        parts = raw_msg.split(maxsplit=2)  # 分割为: ["/mcsm", "restart", "2 3"]
        
        if len(parts) < 3:
            # 没有提供标识符，使用 identifier 参数（向后兼容）
            identifiers = [identifier.strip()] if identifier.strip() else []
        else:
            # 提取所有标识符（支持空格分隔的多个标识符）
            identifiers = [ident.strip() for ident in parts[2].strip().split() if ident.strip()]
        
        # 批量操作
        if len(identifiers) > 1:
            instances, failed_identifiers = self._collect_instances_for_batch(identifiers)
            
            if instances is None:  # 类型不一致
                yield event.plain_result(f"❌ 批量操作时所有标识符必须是同一类型（编号/UUID/名称），当前混合使用了不同类型")
                return
            
            if not instances:
                yield event.plain_result(f"❌ 批量重启失败: 所有标识符都找不到对应的实例")
                return
            
            # 发送开始消息
            yield event.plain_result(f"🔄 开始批量重启 {len(instances)} 个实例...")
            await asyncio.sleep(self.batch_interval)
            
            # 收集所有操作结果，循环中不 yield
            success_count = 0
            fail_count = 0
            fail_details = []
            result_messages = []  # 收集所有结果消息
            
            for idx, (ident, daemon_id, instance_id, instance_name) in enumerate(instances, 1):
                # 检查冷却
                if self.cooldown_manager.check_cooldown(instance_id):
                    result_messages.append(f"⏳ {instance_name} 操作太快了，跳过")
                    fail_count += 1
                    fail_details.append(f"{instance_name}: 操作太快")
                    await asyncio.sleep(self.batch_interval)  # 保持延迟，但不 yield
                    continue
                
                # 执行 API 请求
                resp = await self.make_mcsm_request(
                    "/protected_instance/restart",
                    method="GET",
                    params={"uuid": instance_id, "daemonId": daemon_id}
                )
                
                if resp.get("status") != 200:
                    err = resp.get("data") or resp.get("error") or "未知错误"
                    status_code = resp.get("status", "???")
                    result_messages.append(f"❌ {instance_name} 重启失败: [{status_code}] {err}")
                    fail_count += 1
                    fail_details.append(f"{instance_name}: {err}")
                else:
                    self.cooldown_manager.set_cooldown(instance_id)
                    result_messages.append(f"✅ {instance_name} 重启命令已发送")
                    success_count += 1
                
                # 每个实例处理完后延迟（除了最后一个）
                if idx < len(instances):
                    await asyncio.sleep(self.batch_interval)
            
            # 循环结束后，一次性发送所有结果
            # 构建完整的结果消息
            result_msg = f"📊 批量重启完成: 成功 {success_count} 个，失败 {fail_count} 个\n\n"
            result_msg += "\n".join(result_messages)
            
            if failed_identifiers:
                result_msg += f"\n\n⚠️ 未找到的标识符: {', '.join(failed_identifiers)}"
            if fail_details:
                result_msg += f"\n\n❌ 失败详情:\n" + "\n".join(fail_details)
            
            yield event.plain_result(result_msg)
            return
        
        # 单实例操作
        if not identifiers:
            yield event.plain_result("❌ 请输入有效的实例标识符")
            return
        
        # 使用第一个标识符（单实例操作）
        async for result in self._process_single_instance(
            event, identifiers[0], "🔄", "重启", "/protected_instance/restart"
        ):
            yield result

    @filter.command("mcsm kill")
    async def mcsm_kill(self, event: AstrMessageEvent, identifier: str):
        """强制终止实例 (支持名称/编号/UUID，支持批量操作)"""
        if not self._check_authorized_for_command(event, "kill"):
            yield event.plain_result("❌ 权限不足")
            return
        # 从完整消息中提取所有标识符
        raw_msg = event.message_str.strip()
        parts = raw_msg.split(maxsplit=2)  # 分割为: ["/mcsm", "kill", "2 3"]
        
        if len(parts) < 3:
            # 没有提供标识符，使用 identifier 参数（向后兼容）
            identifiers = [identifier.strip()] if identifier.strip() else []
        else:
            # 提取所有标识符（支持空格分隔的多个标识符）
            identifiers = [ident.strip() for ident in parts[2].strip().split() if ident.strip()]
        
        # 批量操作
        if len(identifiers) > 1:
            instances, failed_identifiers = self._collect_instances_for_batch(identifiers)
            
            if instances is None:  # 类型不一致
                yield event.plain_result(f"❌ 批量操作时所有标识符必须是同一类型（编号/UUID/名称），当前混合使用了不同类型")
                return
            
            if not instances:
                yield event.plain_result(f"❌ 批量终止失败: 所有标识符都找不到对应的实例")
                return
            
            # 发送开始消息
            yield event.plain_result(f"☠ 开始批量终止 {len(instances)} 个实例...")
            await asyncio.sleep(self.batch_interval)
            
            # 收集所有操作结果，循环中不 yield
            success_count = 0
            fail_count = 0
            fail_details = []
            result_messages = []  # 收集所有结果消息
            
            for idx, (ident, daemon_id, instance_id, instance_name) in enumerate(instances, 1):
                # 检查冷却
                if self.cooldown_manager.check_cooldown(instance_id):
                    result_messages.append(f"⏳ {instance_name} 操作太快了，跳过")
                    fail_count += 1
                    fail_details.append(f"{instance_name}: 操作太快")
                    await asyncio.sleep(self.batch_interval)  # 保持延迟，但不 yield
                    continue
                
                # 执行 API 请求
                resp = await self.make_mcsm_request(
                    "/protected_instance/kill",
                    method="GET",
                    params={"uuid": instance_id, "daemonId": daemon_id}
                )
                
                if resp.get("status") != 200:
                    err = resp.get("data") or resp.get("error") or "未知错误"
                    status_code = resp.get("status", "???")
                    result_messages.append(f"❌ {instance_name} 终止失败: [{status_code}] {err}")
                    fail_count += 1
                    fail_details.append(f"{instance_name}: {err}")
                else:
                    self.cooldown_manager.set_cooldown(instance_id)
                    result_messages.append(f"✅ {instance_name} 终止命令已发送")
                    success_count += 1
                
                # 每个实例处理完后延迟（除了最后一个）
                if idx < len(instances):
                    await asyncio.sleep(self.batch_interval)
            
            # 循环结束后，一次性发送所有结果
            # 构建完整的结果消息
            result_msg = f"📊 批量终止完成: 成功 {success_count} 个，失败 {fail_count} 个\n\n"
            result_msg += "\n".join(result_messages)
            
            if failed_identifiers:
                result_msg += f"\n\n⚠️ 未找到的标识符: {', '.join(failed_identifiers)}"
            if fail_details:
                result_msg += f"\n\n❌ 失败详情:\n" + "\n".join(fail_details)
            
            yield event.plain_result(result_msg)
            return
        
        # 单实例操作
        if not identifiers:
            yield event.plain_result("❌ 请输入有效的实例标识符")
            return
        
        # 使用第一个标识符（单实例操作）
        async for result in self._process_single_instance(
            event, identifiers[0], "☠", "终止", "/protected_instance/kill"
        ):
            yield result

    @filter.command("mcsm cmd")
    async def mcsm_cmd(self, event: AstrMessageEvent, identifier: str):
        """发送命令 (支持名称/编号/UUID)"""
        if not self._check_authorized_for_command(event, "cmd"):
            yield event.plain_result("❌ 权限不足")
            return
        raw_msg = event.message_str.strip()
        parts = raw_msg.split(maxsplit=3)
        
        if len(parts) < 4:
            yield event.plain_result("⚠️ 参数不足。用法: /mcsm cmd [名称/编号] [命令内容]")
            return
        
        # parts[0]=/mcsm, parts[1]=cmd, parts[2]=identifier, parts[3]=命令内容
        full_command = parts[3].strip()

        # Lookup instance by identifier
        ids = self._get_instance_by_identifier(identifier)
        if not ids:
             # 检查是否是重名导致的查找失败
            if identifier in self.instance_data.get("ambiguous_names", set()):
                 yield event.plain_result(f"❌ 发送失败: 实例名称 '{identifier}' 重复。请使用 /mcsm list 中的 编号/UUID 进行操作。")
            else:
                 yield event.plain_result(f"❌ 找不到实例: {identifier}。请确认名称、编号/UUID 正确，并先运行 /mcsm list 更新列表。")
            return
        
        daemon_id, instance_id = ids

        # Fetch instance name for better messaging
        instance_name = identifier
        try:
            for data in self.instance_data.get("instances", []):
                if data['uuid'] == instance_id:
                    instance_name = data['name']
                    break
        except Exception:
            pass # Use identifier if lookup fails
        
        yield event.plain_result(f"📢 正在向 {instance_name} 发送命令: {full_command}")

        cmd_resp = await self.make_mcsm_request(
            "/protected_instance/command",
            method="GET",
            params={
                "uuid": instance_id,
                "daemonId": daemon_id,
                "command": full_command
            }
        )

        if cmd_resp.get("status") != 200:
            err = cmd_resp.get("data") or cmd_resp.get("error") or "未知错误"
            status_code = cmd_resp.get("status", "???")
            yield event.plain_result(f"❌ 发送失败: [{status_code}] {err}")
            return

        await asyncio.sleep(1) 

        output_resp = await self.make_mcsm_request(
            "/protected_instance/outputlog",
            method="GET",
            params={"uuid": instance_id, "daemonId": daemon_id}
        )

        output = "无返回数据"
        if output_resp.get("status") == 200:
            output_data = output_resp.get("data")
            if output_data and isinstance(output_data, str):
                output = output_data or "无最新日志"
        
        if isinstance(output, str) and len(output) > 500:
            output = "..." + output[-500:]

        yield event.plain_result(f"✅ 命令已发送\n📝 最近日志:\n{output}")

    @filter.command("mcsm log")
    async def mcsm_log(self, event: AstrMessageEvent, identifier: str):
        """查看最近日志 (支持名称/编号/UUID)"""
        if not self._check_authorized_for_command(event, "log"):
            yield event.plain_result("❌ 权限不足")
            return
        ids = self._get_instance_by_identifier(identifier)
        if not ids:
            if identifier in self.instance_data.get("ambiguous_names", set()):
                 yield event.plain_result(f"❌ 获取失败: 实例名称 '{identifier}' 重复。请使用 编号/UUID。")
            else:
                 yield event.plain_result(f"❌ 找不到实例: {identifier}。")
            return
        
        daemon_id, instance_id = ids
        
        log_size = self.config.get("log_size")

        yield event.plain_result(f"📄 正在获取 {identifier} 的最近 {log_size} 条日志...")

        output_resp = await self.make_mcsm_request(
            "/protected_instance/outputlog",
            method="GET",
            params={"uuid": instance_id, "daemonId": daemon_id}
        )

        if output_resp.get("status") != 200:
            err = output_resp.get("error") or "未知错误"
            yield event.plain_result(f"❌ 获取日志失败: {err}")
            return

        log_data = output_resp.get("data", "")
        if not log_data:
            yield event.plain_result("📝 该实例当前没有最新日志。")
            return

        # 处理日志行数
        lines = log_data.strip().split('\n')
        if len(lines) > log_size:
            lines = lines[-log_size:]
        
        formatted_log = "\n".join(lines)
        
        # 长度防爆（可自行调整）
        if len(formatted_log) > 15000:
            formatted_log = "..." + formatted_log[-15000:]

        yield event.plain_result(f"📝 最近日志 ({len(lines)} 条):\n{formatted_log}")

    @filter.command("mcsm status")
    async def mcsm_status(self, event: AstrMessageEvent):
        """查看面板状态"""
        if not self._check_authorized_for_command(event, "status"):
            yield event.plain_result("❌ 权限不足")
            return
        def format_memory_gb(bytes_value):
            if not isinstance(bytes_value, (int, float)) or bytes_value <= 0:
                return "0.00 GB"
            gb = bytes_value / (1024 * 1024 * 1024)
            return f"{gb:.2f} GB"
        
        overview_resp = await self.make_mcsm_request("/overview")
        if overview_resp.get("status") != 200:
            err_msg = overview_resp.get('error', '未知连接错误，请检查配置')
            yield event.plain_result(f"❌ 获取状态失败: {err_msg}")
            return

        data = overview_resp.get("data", {})
        filtered_nodes = self.config.get("filtered_nodes", [])

        total_instances = 0
        running_instances = 0
        visible_node_count = 0
        visible_node_avail = 0

        mcsm_version = data.get("version", "未知版本")

        panel_timestamp_ms = overview_resp.get("time")
        panel_time_formatted = "未知时间"
        if panel_timestamp_ms and isinstance(panel_timestamp_ms, (int, float)):
            try:
                dt_object = datetime.datetime.fromtimestamp(panel_timestamp_ms / 1000.0)
                panel_time_formatted = dt_object.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                panel_time_formatted = "时间戳错误"

        os_system_uptime = data.get("system", {}).get("uptime")
        os_uptime_formatted = format_uptime_seconds(os_system_uptime)
        logger.info(f"OS/Server raw uptime (from panel system): {os_system_uptime} seconds")

        status_text = (
            f"📊 MCSM v{mcsm_version} 状态概览:\n"
            f"  - 数据时间: {panel_time_formatted}\n"
            "----------------------\n"
        )

        if "remote" in data:
            for i, node in enumerate(data["remote"]):
                node_uuid = node.get("uuid")
                if node_uuid in filtered_nodes:
                    continue
                visible_node_count += 1
                if node.get("available"):
                    visible_node_avail += 1
                node_sys = node.get("system", {})
                inst_info = node.get("instance", {})
                total_instances += inst_info.get("total", 0)
                running_instances += inst_info.get("running", 0)

                node_name = node.get("remarks") or node.get("hostname") or f"Unnamed Node ({i+1})"
                node_version = node.get("version", "未知")
                os_version = node_sys.get("version") or node_sys.get("release") or "未知"
                node_cpu_percent = f"{(node_sys.get('cpuUsage', 0) * 100):.2f}%"
                mem_total_bytes = node_sys.get("totalmem", 0)
                mem_usage_ratio = node_sys.get("memUsage", 0)
                mem_used_bytes = mem_total_bytes * mem_usage_ratio
                mem_used_formatted = format_memory_gb(mem_used_bytes)
                mem_total_formatted = format_memory_gb(mem_total_bytes)
                inst_running = inst_info.get("running", 0)
                inst_total = inst_info.get("total", 0)

                status_text += (
                    f"🖥️ 节点: {node_name}\n"
                    f"- 状态: {'🟢 在线' if node.get('available') else '🔴 离线'}\n"
                    f"- 节点版本: {node_version}\n"
                    f"- OS 版本: {os_version}\n"
                    f"- CPU 占用: {node_cpu_percent}\n"
                    f"- 内存占用: {mem_used_formatted} / {mem_total_formatted}\n"
                    f"- 实例数量: {inst_running} 运行中 / {inst_total} 总数\n"
                    "----------------------\n"
                )

        status_text += (
            f"- 在线时间: {os_uptime_formatted}\n"
            f"总节点状态: {visible_node_avail} 在线 / {visible_node_count} 总数\n"
            f"实例运行状态: {running_instances} / {total_instances}\n"
            f"提示: 使用 /mcsm list 查看详情"
        )

        yield event.plain_result(status_text)
