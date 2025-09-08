#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试前端页面稳定性
"""

import urllib.request
import time
import json

def test_frontend_stability():
    """测试前端页面的稳定性"""
    print("开始测试前端页面稳定性...")
    
    # 测试页面加载
    try:
        print("1. 测试页面加载...")
        response = urllib.request.urlopen('http://localhost:8000', timeout=10)
        html_content = response.read().decode('utf-8')
        if 'TRON USDT 索引器' in html_content:
            print("   ✅ 页面加载成功")
        else:
            print("   ❌ 页面内容异常")
            return False
    except Exception as e:
        print(f"   ❌ 页面加载失败: {e}")
        return False
    
    # 测试API响应
    try:
        print("2. 测试API响应...")
        response = urllib.request.urlopen('http://localhost:8080/api/v1/indexer/latest-block', timeout=10)
        api_data = json.loads(response.read().decode('utf-8'))
        if 'latest_block' in api_data:
            print(f"   ✅ API响应正常: {api_data}")
        else:
            print("   ❌ API响应格式异常")
            return False
    except Exception as e:
        print(f"   ❌ API请求失败: {e}")
        return False
    
    # 模拟多次页面访问（模拟刷新）
    print("3. 测试页面刷新稳定性...")
    for i in range(5):
        try:
            print(f"   第 {i+1} 次访问...")
            response = urllib.request.urlopen('http://localhost:8000', timeout=5)
            if response.getcode() == 200:
                print(f"   ✅ 第 {i+1} 次访问成功")
            else:
                print(f"   ❌ 第 {i+1} 次访问失败，状态码: {response.getcode()}")
                return False
            time.sleep(1)  # 等待1秒
        except Exception as e:
            print(f"   ❌ 第 {i+1} 次访问异常: {e}")
            return False
    
    print("\n🎉 所有测试通过！前端页面稳定性良好。")
    return True

if __name__ == "__main__":
    success = test_frontend_stability()
    if not success:
        exit(1)