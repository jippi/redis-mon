package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/go-redis/redis"
	"github.com/hashicorp/consul/api"
)

func main() {
	consul, err := api.NewClient(api.DefaultConfig())
	if err != nil {
		log.Fatal(err)
	}

	services, _, err := consul.Catalog().Service("redis-ha", "", &api.QueryOptions{})
	if err != nil {
		log.Fatal(err)
	}

	var wg sync.WaitGroup
	stopCh := make(chan interface{})
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		fmt.Println()
		close(stopCh)
	}()

	for _, service := range services {
		wg.Add(1)
		go mon(service, stopCh, &wg)
	}

	spew.Dump("ok")
	wg.Wait()
}

func mon(service *api.CatalogService, stopCh chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()

	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", service.TaggedAddresses["lan"], service.ServicePort),
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	t := time.NewTicker(10 * time.Millisecond)

	var downSince *time.Time
	var resyncTime *time.Time
	lastError := fmt.Errorf("")
	role := ""
	roleLog := make([]string, 0)

	for {
		select {
		case <-stopCh:
			fmt.Printf("%s [%s:%d] role log: %s \n", time.Now(), service.TaggedAddresses["lan"], service.ServicePort, strings.Join(roleLog, " -> "))
			return

		case <-t.C:
			k := client.Keys("*")
			is, err := k.Result()
			if err != nil {
				if err.Error() == lastError.Error() {
					continue
				}

				lastError = err

				if downSince == nil {
					n := time.Now()
					downSince = &n
				}

				fmt.Printf("%s [%s:%d] down: %s\n", time.Now(), service.TaggedAddresses["lan"], service.ServicePort, err)
				continue
			}

			m := client.Role()
			r, err := m.Result()
			if err != nil {
				continue
			}
			tr := fmt.Sprintf("%s", r[0])
			if tr == "slave" {
				tr = fmt.Sprintf("%s of %s:%d", tr, r[1], r[2])
			}
			if tr != role {
				role = tr
				fmt.Printf("%s [%s:%d] role change: %s\n", time.Now(), service.TaggedAddresses["lan"], service.ServicePort, role)
				roleLog = append(roleLog, role)
			}

			// currently
			if downSince != nil || resyncTime != nil {
				if resyncTime != nil {
					if len(is) == 15002 {
						fmt.Printf("%s [%s:%d] sync done (sync: %s) \n", time.Now(), service.TaggedAddresses["lan"], service.ServicePort, time.Now().Sub(*resyncTime).String())
						resyncTime = nil
					}

					continue
				}

				if downSince != nil {
					fmt.Printf("%s [%s:%d] back up (%s) \n", time.Now(), service.TaggedAddresses["lan"], service.ServicePort, time.Now().Sub(*downSince).String())
					downSince = nil

					n := time.Now()
					resyncTime = &n
					continue
				}

				continue
			}
		}
	}
}
