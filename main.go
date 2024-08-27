package main

import "github.com/wjewell3/gdelt/functions"

func main() {
    // Call the function using the package name
    gdeltetl.Gdeltetl()
}

// import (
//     "context"
//     "fmt"

//     "google.golang.org/api/drive/v3"
//     "google.golang.org/api/option"
//     "cloud.google.com/go/secretmanager/apiv1"
//     secretmanagerpb "google.golang.org/genproto/googleapis/cloud/secretmanager/v1"
// )

// func main() {
//     ctx := context.Background()

//     // Create the Secret Manager client
//     client, err := secretmanager.NewClient(ctx)
//     if err != nil {
//         fmt.Println("Failed to create secretmanager client:", err)
//         return
//     }
//     defer client.Close()

//     // Define the project, secret, and version
//     projectID := "gdelt-433201"
//     secretID := "gdelt-2"
//     version := "1"

//     // Build the request
//     name := fmt.Sprintf("projects/%s/secrets/%s/versions/%s", projectID, secretID, version)
//     fmt.Println("Secret name:", name) // Debug line to check the formatted name
//     req := &secretmanagerpb.AccessSecretVersionRequest{
//         Name: name,
//     }

//     // Access the secret
//     result, err := client.AccessSecretVersion(ctx, req)
//     if err != nil {
//         fmt.Println("AccessSecretVersion failed:", err)
//         return
//     }

//     // Return the secret payload
//     secret := string(result.Payload.Data)
//     fmt.Println("Secret payload:", secret) // Debug line to check the secret payload

//     clientOption := option.WithCredentialsFile(secret)

//     driveService, err := drive.NewService(ctx, clientOption)
//     if err != nil {
//         fmt.Println("Error creating Drive client:", err)
//         return
//     }

//     // Example: List files in Drive to use the driveService
//     r, err := driveService.Files.List().PageSize(10).Do()
//     if err != nil {
//         fmt.Println("Unable to retrieve files:", err)
//         return
//     }

//     fmt.Println("Files:")
//     for _, i := range r.Files {
//         fmt.Printf("%s (%s)\n", i.Name, i.Id)
//     }
// }
