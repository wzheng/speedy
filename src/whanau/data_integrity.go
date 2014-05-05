package whanau

import "fmt"
import "strings"
import "strconv"
import "crypto"
import "crypto/rsa"
import "crypto/md5"
import "crypto/rand"


// Wrapper functions for signing and verifying
// http://golang.org/pkg/crypto/rsa/#SignPKCS1v15
// http://golang.org/pkg/crypto/rsa/#VerifyPKCS1v15

// value has value and public key, but Sign field is not populated yet
func SignValue(key KeyType, value ValueType, secretKey *rsa.PrivateKey) ([]byte, error) {

  hashMD5 := md5.New()

  // concatenate key, value, and public key to bind them all together
  // sign s
  s := string(key) + strings.Join(value.Servers, string(",")) + value.PubKey.N.String() + strconv.Itoa(value.PubKey.E)
  hashMD5.Write([]byte(s))
  digest := hashMD5.Sum(nil)

  sk, err := rsa.GenerateKey(rand.Reader, 2014);

  if err != nil {
    fmt.Println(err);
  }

  err = sk.Validate();
  if err != nil {
    fmt.Println("Validation failed.", err);
  }

  sig, sigErr := rsa.SignPKCS1v15(rand.Reader, secretKey, crypto.MD5, digest)

  if sigErr != nil {
    fmt.Println("Signing failed.", err);
  }

  return sig, sigErr
}


// TODO
func signTrueValue(key KeyType, value TrueValueType, secretKey rsa.PrivateKey) ([]byte, error) {
  return nil, nil
}

func VerifyValue(key KeyType, value ValueType) bool {

  if value.Sign == nil {
    return false
  }

  hashMD5 := md5.New()

  // concatenate key, value, and public key to bind them all together
  // sign s
  s := string(key) + strings.Join(value.Servers, string(",")) + value.PubKey.N.String() + strconv.Itoa(value.PubKey.E)
  hashMD5.Write([]byte(s))
  digest := hashMD5.Sum(nil)

  err := rsa.VerifyPKCS1v15(value.PubKey, crypto.MD5, digest, value.Sign)
  if err != nil {
    return false
  }

  return true;
}

// TODO
func verifyTrueValue() {
  return
}

