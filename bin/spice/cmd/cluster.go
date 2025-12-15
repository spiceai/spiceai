/*
Copyright 2025 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cmd

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/logrusorgru/aurora"
	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/constants"
)

const (
	caValidityYears     = 10
	clientValidityYears = 1
	caCertFilename      = "ca.crt"
	caKeyFilename       = "ca.key"
	pkiDirName          = "pki"
	caCN                = "Spice.ai CLI Root CA - DO NOT USE IN PRODUCTION"
	defaultOU           = "unknown"
)

var clusterCmd = &cobra.Command{
	Use:   "cluster",
	Short: "Cluster operations for Spice runtime",
}

var tlsCmd = &cobra.Command{
	Use:   "tls",
	Short: "TLS certificate operations for clustered mode",
}

var tlsInitCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize a test PKI infrastructure for clustered mode",
	Long: `Initialize a test PKI infrastructure by generating a new Certificate Authority (CA).

This command creates:
- A CA certificate (~/.spice/pki/ca.crt)
- A CA private key (~/.spice/pki/ca.key)

The CA certificate is valid for 10 years and uses ECDSA P-256.

WARNING: This CA is for development and testing purposes only.
         DO NOT use these certificates in production environments.`,
	Example: `
spice cluster tls init
`,
	Run: func(cmd *cobra.Command, args []string) {
		pkiDir, err := getPKIDir()
		if err != nil {
			slog.Error("failed to determine PKI directory", "error", err)
			os.Exit(1)
		}

		caCertPath := filepath.Join(pkiDir, caCertFilename)
		caKeyPath := filepath.Join(pkiDir, caKeyFilename)

		// Check if CA already exists
		if _, err := os.Stat(caCertPath); err == nil {
			cmd.Print("CA certificate already exists. Overwrite (y/n)? ")
			var confirm string
			_, _ = fmt.Scanf("%s", &confirm)
			if strings.ToLower(strings.TrimSpace(confirm)) != "y" {
				cmd.Println("Aborted.")
				return
			}
		}

		// Create PKI directory if it doesn't exist
		if err := os.MkdirAll(pkiDir, 0700); err != nil {
			slog.Error("failed to create PKI directory", "error", err)
			os.Exit(1)
		}

		ou := getOrganizationalUnit()

		// Generate CA private key
		caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			slog.Error("failed to generate CA private key", "error", err)
			os.Exit(1)
		}

		// Create CA certificate template
		serialNumber, err := generateSerialNumber()
		if err != nil {
			slog.Error("failed to generate serial number", "error", err)
			os.Exit(1)
		}

		notBefore := time.Now()
		notAfter := notBefore.AddDate(caValidityYears, 0, 0)

		caTemplate := x509.Certificate{
			SerialNumber: serialNumber,
			Subject: pkix.Name{
				CommonName:         caCN,
				OrganizationalUnit: []string{ou},
			},
			NotBefore:             notBefore,
			NotAfter:              notAfter,
			KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
			IsCA:                  true,
			BasicConstraintsValid: true,
			MaxPathLen:            0,
			MaxPathLenZero:        true,
		}

		// Create self-signed CA certificate
		caCertDER, err := x509.CreateCertificate(rand.Reader, &caTemplate, &caTemplate, &caKey.PublicKey, caKey)
		if err != nil {
			slog.Error("failed to create CA certificate", "error", err)
			os.Exit(1)
		}

		// Write CA certificate
		if err := writePEMFile(caCertPath, "CERTIFICATE", caCertDER); err != nil {
			slog.Error("failed to write CA certificate", "error", err)
			os.Exit(1)
		}

		// Write CA private key
		caKeyDER, err := x509.MarshalECPrivateKey(caKey)
		if err != nil {
			slog.Error("failed to marshal CA private key", "error", err)
			os.Exit(1)
		}
		if err := writePEMFile(caKeyPath, "EC PRIVATE KEY", caKeyDER); err != nil {
			slog.Error("failed to write CA private key", "error", err)
			os.Exit(1)
		}

		cmd.Println()
		cmd.Println(fmt.Sprintf("%s", aurora.BrightGreen("Test PKI infrastructure initialized successfully!")))
		cmd.Println()
		cmd.Println(fmt.Sprintf("CA Certificate: %s", caCertPath))
		cmd.Println(fmt.Sprintf("CA Private Key: %s", caKeyPath))
		cmd.Println(fmt.Sprintf("Validity:       %d years (until %s)", caValidityYears, notAfter.Format("2006-01-02")))
		cmd.Println(fmt.Sprintf("OU:             %s", ou))
		cmd.Println()
		cmd.Println(fmt.Sprintf("%s", aurora.BrightYellow("⚠️  WARNING: This CA is for development and testing only.")))
		cmd.Println(fmt.Sprintf("%s", aurora.BrightYellow("            DO NOT use these certificates in production!")))
		cmd.Println()
		cmd.Println("Next steps:")
		cmd.Println(fmt.Sprintf("  Run %s to create a certificate for a cluster member.", aurora.BrightCyan("spice cluster tls add <client-name>")))
	},
}

var tlsAddCmd = &cobra.Command{
	Use:   "add <client-name>",
	Short: "Create a new client certificate signed by the CA",
	Long: `Create a new client certificate and private key signed by the CA.

This command creates:
- A client certificate (~/.spice/pki/<client-name>.crt)
- A client private key (~/.spice/pki/<client-name>.key)

The client certificate is valid for 1 year and uses ECDSA P-256.
The certificate includes localhost and 127.0.0.1 as Subject Alternative Names (SANs).

The CA must be initialized first using 'spice cluster tls init'.`,
	Example: `
spice cluster tls add node1
spice cluster tls add my-spice-instance
spice cluster tls add node1 --host myserver.example.com
`,
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		clientName := args[0]

		if clientName == "" {
			slog.Error("client name cannot be empty")
			os.Exit(1)
		}

		// Validate client name (alphanumeric, hyphens, underscores)
		for _, c := range clientName {
			if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '-' && c != '_' {
				slog.Error("client name can only contain letters, numbers, hyphens, and underscores")
				os.Exit(1)
			}
		}

		pkiDir, err := getPKIDir()
		if err != nil {
			slog.Error("failed to determine PKI directory", "error", err)
			os.Exit(1)
		}

		caCertPath := filepath.Join(pkiDir, caCertFilename)
		caKeyPath := filepath.Join(pkiDir, caKeyFilename)
		clientCertPath := filepath.Join(pkiDir, clientName+".crt")
		clientKeyPath := filepath.Join(pkiDir, clientName+".key")

		// Check if CA exists
		if _, err := os.Stat(caCertPath); os.IsNotExist(err) {
			slog.Error("CA certificate not found. Run 'spice cluster tls init' first.")
			os.Exit(1)
		}
		if _, err := os.Stat(caKeyPath); os.IsNotExist(err) {
			slog.Error("CA private key not found. Run 'spice cluster tls init' first.")
			os.Exit(1)
		}

		// Check if client cert already exists
		if _, err := os.Stat(clientCertPath); err == nil {
			cmd.Printf("Certificate for '%s' already exists. Overwrite (y/n)? ", clientName)
			var confirm string
			_, _ = fmt.Scanf("%s", &confirm)
			if strings.ToLower(strings.TrimSpace(confirm)) != "y" {
				cmd.Println("Aborted.")
				return
			}
		}

		// Load CA certificate
		caCertPEM, err := os.ReadFile(caCertPath)
		if err != nil {
			slog.Error("failed to read CA certificate", "error", err)
			os.Exit(1)
		}
		caCertBlock, _ := pem.Decode(caCertPEM)
		if caCertBlock == nil {
			slog.Error("failed to decode CA certificate PEM")
			os.Exit(1)
		}
		caCert, err := x509.ParseCertificate(caCertBlock.Bytes)
		if err != nil {
			slog.Error("failed to parse CA certificate", "error", err)
			os.Exit(1)
		}

		// Load CA private key
		caKeyPEM, err := os.ReadFile(caKeyPath)
		if err != nil {
			slog.Error("failed to read CA private key", "error", err)
			os.Exit(1)
		}
		caKeyBlock, _ := pem.Decode(caKeyPEM)
		if caKeyBlock == nil {
			slog.Error("failed to decode CA private key PEM")
			os.Exit(1)
		}
		caKey, err := x509.ParseECPrivateKey(caKeyBlock.Bytes)
		if err != nil {
			slog.Error("failed to parse CA private key", "error", err)
			os.Exit(1)
		}

		// Generate client private key
		clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			slog.Error("failed to generate client private key", "error", err)
			os.Exit(1)
		}

		// Create client certificate template
		serialNumber, err := generateSerialNumber()
		if err != nil {
			slog.Error("failed to generate serial number", "error", err)
			os.Exit(1)
		}

		notBefore := time.Now()
		notAfter := notBefore.AddDate(clientValidityYears, 0, 0)

		// Build SANs - always include localhost and 127.0.0.1
		dnsNames := []string{"localhost"}
		ipAddresses := []net.IP{net.ParseIP("127.0.0.1")}

		// Add optional host to SANs
		hostFlag, _ := cmd.Flags().GetString("host")
		if hostFlag != "" {
			// Check if it's an IP address or DNS name
			if ip := net.ParseIP(hostFlag); ip != nil {
				ipAddresses = append(ipAddresses, ip)
			} else {
				dnsNames = append(dnsNames, hostFlag)
			}
		}

		clientTemplate := x509.Certificate{
			SerialNumber: serialNumber,
			Subject: pkix.Name{
				CommonName: clientName,
			},
			NotBefore:             notBefore,
			NotAfter:              notAfter,
			KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
			ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
			BasicConstraintsValid: true,
			IsCA:                  false,
			DNSNames:              dnsNames,
			IPAddresses:           ipAddresses,
		}

		// Create client certificate signed by CA
		clientCertDER, err := x509.CreateCertificate(rand.Reader, &clientTemplate, caCert, &clientKey.PublicKey, caKey)
		if err != nil {
			slog.Error("failed to create client certificate", "error", err)
			os.Exit(1)
		}

		// Write client certificate
		if err := writePEMFile(clientCertPath, "CERTIFICATE", clientCertDER); err != nil {
			slog.Error("failed to write client certificate", "error", err)
			os.Exit(1)
		}

		// Write client private key
		clientKeyDER, err := x509.MarshalECPrivateKey(clientKey)
		if err != nil {
			slog.Error("failed to marshal client private key", "error", err)
			os.Exit(1)
		}
		if err := writePEMFile(clientKeyPath, "EC PRIVATE KEY", clientKeyDER); err != nil {
			slog.Error("failed to write client private key", "error", err)
			os.Exit(1)
		}

		cmd.Println()
		cmd.Println(fmt.Sprintf("%s", aurora.BrightGreen(fmt.Sprintf("Certificate for '%s' created successfully!", clientName))))
		cmd.Println()
		cmd.Println(fmt.Sprintf("Certificate: %s", clientCertPath))
		cmd.Println(fmt.Sprintf("Private Key: %s", clientKeyPath))
		cmd.Println(fmt.Sprintf("Validity:    %d year (until %s)", clientValidityYears, notAfter.Format("2006-01-02")))
		cmd.Println(fmt.Sprintf("CN:          %s", clientName))
		cmd.Println(fmt.Sprintf("DNS SANs:    %s", strings.Join(dnsNames, ", ")))
		ipStrs := make([]string, len(ipAddresses))
		for i, ip := range ipAddresses {
			ipStrs[i] = ip.String()
		}
		cmd.Println(fmt.Sprintf("IP SANs:     %s", strings.Join(ipStrs, ", ")))
	},
}

// getPKIDir returns the path to the PKI directory (~/.spice/pki)
func getPKIDir() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("failed to get home directory: %w", err)
	}
	return filepath.Join(homeDir, constants.DotSpice, pkiDirName), nil
}

// getOrganizationalUnit attempts to get the user's email from git config,
// falling back to "unknown" if not available
func getOrganizationalUnit() string {
	out, err := exec.Command("git", "config", "--get", "user.email").Output()
	if err != nil {
		return defaultOU
	}
	email := strings.TrimSpace(string(out))
	if email == "" {
		return defaultOU
	}
	return email
}

// generateSerialNumber generates a random serial number for certificates
func generateSerialNumber() (*big.Int, error) {
	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	return rand.Int(rand.Reader, serialNumberLimit)
}

// writePEMFile writes data as a PEM-encoded file with secure permissions
func writePEMFile(path string, pemType string, data []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			slog.Error("failed to close file", "file", path, "error", err)
		}
	}()

	block := &pem.Block{
		Type:  pemType,
		Bytes: data,
	}
	if err := pem.Encode(file, block); err != nil {
		return fmt.Errorf("failed to encode PEM: %w", err)
	}

	// Ensure secure permissions
	if err := file.Chmod(0600); err != nil {
		return fmt.Errorf("failed to set file permissions: %w", err)
	}

	return nil
}

func init() {
	tlsAddCmd.Flags().String("host", "", "Host to include in Subject Alternative Names (e.g., myserver.example.com)")
	tlsCmd.AddCommand(tlsInitCmd)
	tlsCmd.AddCommand(tlsAddCmd)
	clusterCmd.AddCommand(tlsCmd)
	RootCmd.AddCommand(clusterCmd)
}
