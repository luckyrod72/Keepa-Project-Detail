package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
)

func main() {
	// 创建一个默认的 gin 路由引擎
	r := gin.Default()

	// 定义一个处理 Keepa 产品 API 请求的路由
	r.GET("/product", handleKeepaProduct)

	// 运行服务器
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	r.Run(":" + port) // 对于 Gin
}

// 处理 Keepa 产品 API 请求的函数
func handleKeepaProduct(c *gin.Context) {
	// 可以从请求中获取 ASIN 参数，如果没有提供则使用默认值
	asin := c.DefaultQuery("asin", "B0DCDS4D5L")

	// Keepa API 的 URL 和凭证
	url := fmt.Sprintf("https://api.keepa.com/product?domain=1&key=rt7t1904up7638ddhboifgfksfedu7pap6gde8p5to6mtripoib3q4n1h3433rh4&asin=%s&stats=90&update=-1&history=1&days=90&code-limit=10&offers=20&only-live-offers=1&rental=0&videos=0&aplus=0&rating=0&buybox=1&stock=1", asin)
	method := "GET"

	// 创建空的请求体
	payload := strings.NewReader(``)

	// 创建 HTTP 客户端
	client := &http.Client{}

	// 创建请求
	req, err := http.NewRequest(method, url, payload)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("创建请求失败: %v", err),
		})
		return
	}

	// 发送请求
	res, err := client.Do(req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("发送请求失败: %v", err),
		})
		return
	}
	defer res.Body.Close()

	// 读取响应体
	body, err := io.ReadAll(res.Body)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("读取响应失败: %v", err),
		})
		return
	}

	// 将 Keepa API 的响应原样返回给客户端
	c.Data(res.StatusCode, "application/json", body)
}
