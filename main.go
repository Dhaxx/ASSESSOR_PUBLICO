package main

import (
	"ASSESSOR_PUBLICO/COMPRAS"
	// "sync"

	"github.com/vbauerster/mpb/v8"
)

func main() {
	// var wg1 sync.WaitGroup
	// var wg2 sync.WaitGroup
    // var wg3 sync.WaitGroup
    // var wg4 sync.WaitGroup
    // var wg5 sync.WaitGroup
    // var wg6 sync.WaitGroup
    // var wg7 sync.WaitGroup
	// var wg8 sync.WaitGroup
	p := mpb.New()
    
//////////////////////
	// wg1.Add(4)
    // go func() {
    //     defer wg1.Done()
    //     compras.Cadunimedida(p)
    // }()
    // go func() {
    //     defer wg1.Done()
    //     compras.GrupoSubgrupo(p)
    // }()
    // go func() {
    //     defer wg1.Done()
    //     compras.Destino(p)
    // }()
    // go func() {
    //     defer wg1.Done()
    //     compras.CentroCusto(p)
    // }()
	// wg1.Wait()

	// wg2.Add(2)
	// go func() {
	// 	defer wg2.Done()
	// 	compras.Cadest(p)
	// }()
	// go func() {
	// 	defer wg2.Done()
	// 	compras.Cadorc(p)
	// }()
	// wg2.Wait()

	// wg3.Add(2)
	// go func() {
	// 	defer wg3.Done()
	// 	compras.Icadorc(p)
	// }()
	// go func() {
	// 	defer wg3.Done()
	// 	compras.Fcadorc(p)
	// }()
	// wg3.Wait()

	// wg4.Add(2)
	// go func() {
	// 	defer wg4.Done()
	// 	compras.Vcadorc(p)
	// }()
	// go func() {
	// 	defer wg4.Done()
	// 	compras.Cadlic(p)	
	// }()
	// wg4.Wait()

	// compras.Cadprolic(p)

	// wg5.Add(2)
	// go func() {
	// 	defer wg5.Done()
	// 	compras.CadprolicDetalhe()
	// }()
	// go func() {
	// 	defer wg5.Done()
	// 	compras.ProlicProlics(p)
	// }()
	// wg5.Wait()

	// compras.CadproProposta(p)

	// wg6.Add(2)
	// go func() {
	// 	defer wg6.Done()
	// 	compras.CadlicSessao()
	// }()
	// go func() {
	// 	defer wg6.Done()
	// 	compras.CadproLance()
	// }()
	// wg6.Wait()

	// compras.CadproStatus()

	// wg7.Add(2)
	// go func() {
	// 	defer wg7.Done()
	// 	compras.CadproFinal()
	// }()
	// go func() {
	// 	defer wg7.Done()
	// 	compras.Cadpro()
	// }()
	// wg7.Wait()

	// compras.Regpreco()
	// wg8.Add(2)
	// go func() {
	// 	defer wg8.Done()
		compras.Aditivo(p)
	// }()
	// go func() {
	// 	defer wg8.Done()
	// 	compras.Cadped(p)
	// }()
	// wg8.Wait()

	// compras.Icadped(p)
}