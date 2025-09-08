#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简单的HTTP服务器，用于测试前端页面
"""

import http.server
import socketserver
import os
import sys
from urllib.parse import urlparse, parse_qs
import json
import random
from datetime import datetime, timedelta
import urllib.request
import urllib.error

class MockAPIHandler(http.server.SimpleHTTPRequestHandler):
    """模拟API响应的HTTP处理器"""
    
    def do_GET(self):
        parsed_path = urlparse(self.path)
        path = parsed_path.path
        
        # 处理API请求
        if path.startswith('/api/v1/'):
            self.handle_api_request(path, parsed_path.query)
        else:
            # 处理静态文件请求
            super().do_GET()
    
    def handle_api_request(self, path, query):
        """处理API请求 - 代理到真实后端API"""
        try:
            # 构建后端API URL
            backend_url = f"http://localhost:8080{path}"
            if query:
                backend_url += f"?{query}"
            
            # 代理请求到后端
            try:
                with urllib.request.urlopen(backend_url) as response:
                    data = response.read().decode('utf-8')
                    json_data = json.loads(data)
                    self.send_json_response(json_data)
            except urllib.error.URLError as e:
                print(f"Backend API error: {e}")
                # 如果后端不可用，使用Mock数据
                self.handle_mock_api_request(path, query)
        except Exception as e:
            self.send_json_response({'success': False, 'error': str(e)})
    
    def handle_mock_api_request(self, path, query):
        """处理Mock API请求"""
        try:
            if path == '/api/v1/transfers':
                response = self.get_mock_transfers(query)
            elif path == '/api/v1/stats':
                response = self.get_mock_stats()
            elif path == '/api/v1/health':
                response = self.get_mock_health()
            elif path == '/api/v1/system/status':
                response = self.get_mock_system_status()
            elif path == '/api/v1/analytics/trend':
                response = self.get_mock_trend(query)
            elif path == '/api/v1/analytics/volume-distribution':
                response = self.get_mock_volume_distribution()
            else:
                response = {'success': False, 'error': 'API endpoint not found'}
            
            self.send_json_response(response)
        except Exception as e:
            self.send_json_response({'success': False, 'error': str(e)})
    
    def get_mock_transfers(self, query):
        """模拟转账数据"""
        params = parse_qs(query)
        page = int(params.get('page', [1])[0])
        limit = int(params.get('limit', [20])[0])
        
        # 生成模拟转账数据
        transfers = []
        for i in range(limit):
            transfer = {
                'tx_hash': f'0x{random.randint(10**63, 10**64-1):064x}',
                'from_address': f'TR{random.randint(10**32, 10**33-1):033x}',
                'to_address': f'TR{random.randint(10**32, 10**33-1):033x}',
                'amount': round(random.uniform(1, 10000), 6),
                'block_number': 58000000 + random.randint(0, 100000),
                'timestamp': int((datetime.now() - timedelta(minutes=random.randint(0, 1440))).timestamp()),
                'status': random.choice(['confirmed', 'confirmed', 'confirmed', 'pending'])
            }
            transfers.append(transfer)
        
        return {
            'success': True,
            'data': {
                'transfers': transfers,
                'pagination': {
                    'current_page': page,
                    'total_pages': 50,
                    'total_items': 1000,
                    'items_per_page': limit
                }
            }
        }
    
    def get_mock_stats(self):
        """模拟统计数据"""
        return {
            'success': True,
            'data': {
                'total_transfers': random.randint(800000, 1200000),
                'total_volume': round(random.uniform(50000000, 100000000), 2),
                'latest_block': 58000000 + random.randint(0, 100000),
                'daily_transfers': random.randint(5000, 15000),
                'daily_volume': round(random.uniform(1000000, 5000000), 2)
            }
        }
    
    def get_mock_health(self):
        """模拟健康检查"""
        return {
            'success': True,
            'data': {
                'status': 'healthy',
                'sync_status': random.choice(['syncing', 'synced', 'synced', 'synced']),
                'uptime': random.randint(3600, 86400),
                'version': '1.0.0'
            }
        }
    
    def get_mock_system_status(self):
        """模拟系统状态"""
        nodes = [
            {'name': 'TRON 主节点', 'url': 'https://api.trongrid.io', 'status': 'online'},
            {'name': 'TRON 备用节点1', 'url': 'https://api.tronstack.io', 'status': 'online'},
            {'name': 'TRON 备用节点2', 'url': 'https://api.shasta.trongrid.io', 'status': random.choice(['online', 'warning'])},
            {'name': 'TronScan API', 'url': 'https://apilist.tronscan.org', 'status': 'online'}
        ]
        
        return {
            'success': True,
            'data': {
                'nodes': nodes,
                'sync_progress': {
                    'current_block': 58000000 + random.randint(0, 100),
                    'target_block': 58000000 + random.randint(100, 200),
                    'percentage': round(random.uniform(85, 99.9), 1)
                },
                'metrics': {
                    'tps': round(random.uniform(10, 50), 1),
                    'latency': random.randint(50, 200),
                    'memory_usage': round(random.uniform(60, 85), 1),
                    'cpu_usage': round(random.uniform(20, 60), 1)
                }
            }
        }
    
    def get_mock_trend(self, query):
        """模拟趋势数据"""
        params = parse_qs(query)
        hours = int(params.get('hours', [24])[0])
        
        trend_data = []
        for i in range(hours):
            hour_time = datetime.now() - timedelta(hours=hours-i)
            trend_data.append({
                'hour': hour_time.isoformat(),
                'count': random.randint(50, 500),
                'volume': round(random.uniform(10000, 100000), 2)
            })
        
        return {
            'success': True,
            'data': trend_data
        }
    
    def get_mock_volume_distribution(self):
        """模拟交易量分布"""
        return {
            'success': True,
            'data': [
                {'range': '0-100 USDT', 'count': random.randint(1000, 2000)},
                {'range': '100-1K USDT', 'count': random.randint(500, 1000)},
                {'range': '1K-10K USDT', 'count': random.randint(200, 500)},
                {'range': '10K-100K USDT', 'count': random.randint(50, 200)},
                {'range': '100K+ USDT', 'count': random.randint(10, 50)}
            ]
        }
    
    def send_json_response(self, data):
        """发送JSON响应"""
        response_data = json.dumps(data, ensure_ascii=False).encode('utf-8')
        
        self.send_response(200)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.send_header('Content-Length', str(len(response_data)))
        self.end_headers()
        self.wfile.write(response_data)
    
    def log_message(self, format, *args):
        """自定义日志格式"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp}] {format % args}")

def main():
    """启动服务器"""
    port = 8000
    
    # 切换到前端目录
    frontend_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(frontend_dir)
    
    print(f"启动前端开发服务器...")
    print(f"服务器地址: http://localhost:{port}")
    print(f"前端目录: {frontend_dir}")
    print(f"按 Ctrl+C 停止服务器")
    print("-" * 50)
    
    try:
        with socketserver.TCPServer(("", port), MockAPIHandler) as httpd:
            httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n服务器已停止")
    except Exception as e:
        print(f"服务器启动失败: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()