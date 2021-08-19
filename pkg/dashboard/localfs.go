package dashboard

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/valyala/fasthttp"
)

type DashboardLocalFs struct {
	rootDir   string
	indexPath string
	jsPath    string
}

func NewDashboardLocalFs(rootDir string) (*DashboardLocalFs, error) {
	if rootDir == "" {
		cwd, err := os.Getwd()
		if err != nil {
			return nil, fmt.Errorf("failed to get current working directory: %w", err)
		}
		rootDir = filepath.Join(rootDir, cwd)
	}

	if !filepath.IsAbs(rootDir) {
		absPath, err := filepath.Abs(rootDir)
		if err != nil {
			return nil, fmt.Errorf("failed to get absolute path of '%s'", rootDir)
		}
		rootDir = absPath
	}

	if _, err := os.Stat(rootDir); err != nil {
		return nil, fmt.Errorf("rootDir '%s' does not exist: %w", rootDir, err)
	}

	indexPath := filepath.Join(rootDir, "html", "index.html")
	jsPath := filepath.Join(rootDir, "js")

	return &DashboardLocalFs{
		rootDir:   rootDir,
		indexPath: indexPath,
		jsPath:    jsPath,
	}, nil
}

func (d *DashboardLocalFs) IndexHandler(ctx *fasthttp.RequestCtx) {
	ctx.Response.Header.SetContentType("text/html")
	fasthttp.ServeFile(ctx, d.indexPath)
}

func (d *DashboardLocalFs) JsHandler(ctx *fasthttp.RequestCtx) {
	jsFile := ctx.UserValue("jsFile").(string)

	jsFilePath := filepath.Join(d.jsPath, jsFile)

	ctx.Response.Header.SetContentType("application/javascript")
	fasthttp.ServeFile(ctx, jsFilePath)
}
