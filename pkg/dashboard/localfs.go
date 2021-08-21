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

	indexPath := filepath.Join(rootDir, "index.html")

	return &DashboardLocalFs{
		rootDir:   rootDir,
		indexPath: indexPath,
	}, nil
}

func (d *DashboardLocalFs) IndexHandler(ctx *fasthttp.RequestCtx) {
	contentType := GetContentType("html")
	ctx.Response.Header.SetContentType(contentType)
	fasthttp.ServeFile(ctx, d.indexPath)
}

func (d *DashboardLocalFs) JsHandler(ctx *fasthttp.RequestCtx) {
	d.fileHandler(ctx, "js")
}

func (d *DashboardLocalFs) CssHandler(ctx *fasthttp.RequestCtx) {
	d.fileHandler(ctx, "css")
}

func (d *DashboardLocalFs) SvgHandler(ctx *fasthttp.RequestCtx) {
	d.fileHandler(ctx, "svg")
}

func (d *DashboardLocalFs) fileHandler(ctx *fasthttp.RequestCtx, filetype string) {
	filePath := ctx.UserValue("filepath").(string)

	subfolder := filetype
	if filetype == "svg" {
		subfolder = "media"
	}

	fullFilePath := filepath.Join(d.rootDir, "static", subfolder, filePath)

	contentType := GetContentType(filetype)
	ctx.Response.Header.SetContentType(contentType)
	fasthttp.ServeFile(ctx, fullFilePath)
}
