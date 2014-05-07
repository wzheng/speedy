package whanau

import "fmt"
import "strconv"
import "crypto"
import "crypto/rsa"
import "crypto/md5"
import "crypto/rand"

// Wrapper functions for signing and verifying
// http://golang.org/pkg/crypto/rsa/#SignPKCS1v15
// http://golang.org/pkg/crypto/rsa/#VerifyPKCS1v15

// value has value and public key, but Sign field is not populated yet
func SignTrueValue(value TrueValueType, secretKey *rsa.PrivateKey) ([]byte, error) {

	hashMD5 := md5.New()

	// concatenate key, value, and public key to bind them all together
	// sign s
	s := value.TrueValue + value.Originator + value.PubKey.N.String() + strconv.Itoa(value.PubKey.E)
	hashMD5.Write([]byte(s))
	digest := hashMD5.Sum(nil)

	sig, sigErr := rsa.SignPKCS1v15(rand.Reader, secretKey, crypto.MD5, digest)

	if sigErr != nil {
		fmt.Println("Signing failed.", sigErr)
	}

	return sig, sigErr
}

func VerifyTrueValue(value TrueValueType) bool {

	if (value.Sign == nil) || (value.PubKey == nil) {
		return false
	}

	hashMD5 := md5.New()

	// concatenate key, value, and public key to bind them all together
	// sign s
	s := value.TrueValue + value.Originator + value.PubKey.N.String() + strconv.Itoa(value.PubKey.E)
	hashMD5.Write([]byte(s))
	digest := hashMD5.Sum(nil)

	err := rsa.VerifyPKCS1v15(value.PubKey, crypto.MD5, digest, value.Sign)
	if err != nil {
		return false
	}

	return true
}
