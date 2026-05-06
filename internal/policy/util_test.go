package policy

import "os"

// osWriteFile is a tiny shim so the engine_test does not import os
// directly — it keeps engine_test focused on policy assertions.
func osWriteFile(path string, body []byte, mode os.FileMode) error {
	return os.WriteFile(path, body, mode)
}
