package utils

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/big"
	"regexp"
	"strings"
)

// TronAddressPrefix 波场地址前缀
const TronAddressPrefix = "41"

// Base58 字符集
const base58Alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

// ValidateTronAddress 验证波场地址格式
func ValidateTronAddress(address string) bool {
	// 波场地址以T开头，长度为34位
	if len(address) != 34 || !strings.HasPrefix(address, "T") {
		return false
	}

	// 检查是否为有效的Base58字符
	for _, char := range address {
		if !strings.ContainsRune(base58Alphabet, char) {
			return false
		}
	}

	return true
}

// HexToTronAddress 将十六进制地址转换为波场地址
func HexToTronAddress(hexAddress string) (string, error) {
	// 移除0x前缀
	hexAddress = strings.TrimPrefix(hexAddress, "0x")

	// 确保地址长度正确
	if len(hexAddress) != 40 {
		return "", fmt.Errorf("invalid hex address length: %d", len(hexAddress))
	}

	// 添加波场地址前缀
	fullHex := TronAddressPrefix + hexAddress

	// 转换为字节数组
	addrBytes, err := hex.DecodeString(fullHex)
	if err != nil {
		return "", fmt.Errorf("failed to decode hex address: %v", err)
	}

	// 计算校验和
	checksum := doubleSha256(addrBytes)

	// 添加校验和
	fullAddr := append(addrBytes, checksum[:4]...)

	// Base58编码
	return base58Encode(fullAddr), nil
}

// TronAddressToHex 将波场地址转换为十六进制地址
func TronAddressToHex(address string) (string, error) {
	if !ValidateTronAddress(address) {
		return "", fmt.Errorf("invalid Tron address: %s", address)
	}

	// Base58解码
	decoded := base58Decode(address)
	if len(decoded) != 25 {
		return "", fmt.Errorf("invalid decoded address length: %d", len(decoded))
	}

	// 验证校验和
	addr := decoded[:21]
	checksum := decoded[21:]
	expectedChecksum := doubleSha256(addr)

	for i := 0; i < 4; i++ {
		if checksum[i] != expectedChecksum[i] {
			return "", fmt.Errorf("invalid address checksum")
		}
	}

	// 移除前缀并返回十六进制
	return hex.EncodeToString(addr[1:]), nil
}

// doubleSha256 双重SHA256哈希
func doubleSha256(data []byte) []byte {
	first := sha256.Sum256(data)
	second := sha256.Sum256(first[:])
	return second[:]
}

// base58Encode Base58编码
func base58Encode(input []byte) string {
	if len(input) == 0 {
		return ""
	}

	// 计算前导零的数量
	zeroCount := 0
	for i := 0; i < len(input) && input[i] == 0; i++ {
		zeroCount++
	}

	// 转换为大整数
	num := new(big.Int).SetBytes(input)
	base := big.NewInt(58)
	zero := big.NewInt(0)

	// 进行Base58编码
	var result []byte
	for num.Cmp(zero) > 0 {
		mod := new(big.Int)
		num.DivMod(num, base, mod)
		result = append([]byte{base58Alphabet[mod.Int64()]}, result...)
	}

	// 添加前导1（对应前导零）
	for i := 0; i < zeroCount; i++ {
		result = append([]byte{base58Alphabet[0]}, result...)
	}

	return string(result)
}

// base58Decode Base58解码
func base58Decode(input string) []byte {
	if len(input) == 0 {
		return nil
	}

	// 计算前导1的数量
	zeroCount := 0
	for i := 0; i < len(input) && input[i] == base58Alphabet[0]; i++ {
		zeroCount++
	}

	// 转换为大整数
	num := big.NewInt(0)
	base := big.NewInt(58)

	for _, char := range input {
		pos := strings.IndexRune(base58Alphabet, char)
		if pos == -1 {
			return nil
		}
		num.Mul(num, base)
		num.Add(num, big.NewInt(int64(pos)))
	}

	// 转换为字节数组
	bytes := num.Bytes()

	// 添加前导零
	for i := 0; i < zeroCount; i++ {
		bytes = append([]byte{0}, bytes...)
	}

	return bytes
}

// ParseUSDTAmount 解析USDT金额（6位小数）
func ParseUSDTAmount(amountStr string) (*big.Int, error) {
	// 移除可能的空格
	amountStr = strings.TrimSpace(amountStr)

	// 检查是否包含小数点
	if strings.Contains(amountStr, ".") {
		parts := strings.Split(amountStr, ".")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid amount format: %s", amountStr)
		}

		integerPart := parts[0]
		decimalPart := parts[1]

		// USDT有6位小数
		if len(decimalPart) > 6 {
			return nil, fmt.Errorf("too many decimal places: %s", amountStr)
		}

		// 补齐到6位小数
		for len(decimalPart) < 6 {
			decimalPart += "0"
		}

		// 组合整数部分和小数部分
		fullAmount := integerPart + decimalPart
		amount, ok := new(big.Int).SetString(fullAmount, 10)
		if !ok {
			return nil, fmt.Errorf("failed to parse amount: %s", amountStr)
		}

		return amount, nil
	} else {
		// 没有小数点，直接解析并乘以10^6
		amount, ok := new(big.Int).SetString(amountStr, 10)
		if !ok {
			return nil, fmt.Errorf("failed to parse amount: %s", amountStr)
		}

		// 乘以10^6转换为最小单位
		multiplier := new(big.Int).Exp(big.NewInt(10), big.NewInt(6), nil)
		return amount.Mul(amount, multiplier), nil
	}
}

// FormatUSDTAmount 格式化USDT金额为可读格式
func FormatUSDTAmount(amount *big.Int) string {
	if amount == nil {
		return "0.00"
	}

	// USDT有6位小数
	divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(6), nil)

	// 计算整数部分和余数
	integerPart := new(big.Int).Div(amount, divisor)
	remainder := new(big.Int).Mod(amount, divisor)

	// 如果余数为0，返回整数部分加.00
	if remainder.Cmp(big.NewInt(0)) == 0 {
		return fmt.Sprintf("%s.00", integerPart.String())
	}

	// 格式化小数部分
	decimalStr := fmt.Sprintf("%06d", remainder.Uint64())

	// 移除尾随的零，但至少保留2位小数
	decimalStr = strings.TrimRight(decimalStr, "0")
	if len(decimalStr) < 2 {
		decimalStr = fmt.Sprintf("%02s", decimalStr)
	}

	return fmt.Sprintf("%s.%s", integerPart.String(), decimalStr)
}

// IsValidTxHash 验证交易哈希格式
func IsValidTxHash(txHash string) bool {
	// 波场交易哈希是64位十六进制字符串
	if len(txHash) != 64 {
		return false
	}

	// 检查是否为有效的十六进制
	matched, _ := regexp.MatchString("^[0-9a-fA-F]{64}$", txHash)
	return matched
}

// GenerateContractMethodID 生成智能合约方法ID
func GenerateContractMethodID(methodSignature string) string {
	// 计算方法签名的Keccak256哈希，取前4字节
	// 这里使用SHA256作为简化实现
	hash := sha256.Sum256([]byte(methodSignature))
	return hex.EncodeToString(hash[:4])
}

// EncodeTransferData 编码USDT转账数据
func EncodeTransferData(toAddress string, amount *big.Int) (string, error) {
	// 简化的ABI编码实现
	// 实际实现需要完整的ABI编码库

	// transfer方法ID
	methodID := "a9059cbb" // transfer(address,uint256)的方法ID

	// 转换地址为十六进制
	toHex, err := TronAddressToHex(toAddress)
	if err != nil {
		return "", err
	}

	// 补齐地址到32字节
	paddedAddress := fmt.Sprintf("%064s", toHex)

	// 补齐金额到32字节
	amountHex := fmt.Sprintf("%064s", amount.Text(16))

	// 组合数据
	data := methodID + paddedAddress + amountHex

	return data, nil
}

// DecodeTransferData 解码USDT转账数据
func DecodeTransferData(data string) (toAddress string, amount *big.Int, err error) {
	// 移除0x前缀
	data = strings.TrimPrefix(data, "0x")

	// 检查数据长度（4字节方法ID + 32字节地址 + 32字节金额）
	if len(data) < 136 {
		return "", nil, fmt.Errorf("invalid transfer data length: %d", len(data))
	}

	// 提取方法ID
	methodID := data[:8]
	if methodID != "a9059cbb" {
		return "", nil, fmt.Errorf("not a transfer method call")
	}

	// 提取地址（跳过前导零）
	addressHex := data[8:72]
	addressHex = strings.TrimLeft(addressHex, "0")
	if len(addressHex)%2 != 0 {
		addressHex = "0" + addressHex
	}

	// 转换为波场地址
	toAddress, err = HexToTronAddress(addressHex)
	if err != nil {
		return "", nil, fmt.Errorf("failed to convert address: %v", err)
	}

	// 提取金额
	amountHex := data[72:136]
	amount = new(big.Int)
	amount.SetString(amountHex, 16)

	return toAddress, amount, nil
}

// TronUtils 波场工具集合
type TronUtils struct{}

// NewTronUtils 创建波场工具实例
func NewTronUtils() *TronUtils {
	return &TronUtils{}
}

// GetMethodSignature 获取常用方法签名
func (tu *TronUtils) GetMethodSignature(method string) string {
	signatures := map[string]string{
		"transfer":    "transfer(address,uint256)",
		"balanceOf":   "balanceOf(address)",
		"approve":     "approve(address,uint256)",
		"allowance":   "allowance(address,address)",
		"totalSupply": "totalSupply()",
	}

	if sig, exists := signatures[method]; exists {
		return sig
	}
	return ""
}
