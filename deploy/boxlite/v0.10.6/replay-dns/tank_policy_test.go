package dns

import (
    "net"
    "testing"
    "github.com/containers/gvisor-tap-vsock/pkg/types"
    "github.com/miekg/dns"
)

func TestTankReplayForwardedQuestionsRefused(t *testing.T) {
    // These call the real answer path with forwarding disabled. The test
    // container has no network; no resolver stub or mock DNS answer is used.
    for _, queryType := range []uint16{dns.TypeA,dns.TypeAAAA,dns.TypeCNAME,dns.TypeMX,dns.TypeNS,dns.TypeSRV,dns.TypeTXT,dns.TypeANY,dns.TypeAXFR,dns.TypeNULL,0,65535} {
        for _, name := range []string{"example.com.","exfil.attacker.invalid.","host.boxlite.internal.attacker.invalid.","\000.attacker.invalid."} {
            for _, class := range []uint16{dns.ClassINET,dns.ClassCHAOS,dns.ClassANY} {
                h:=dnsHandler{forwardingDisabled:true};m:=new(dns.Msg)
                m.Question=[]dns.Question{{Name:name,Qtype:queryType,Qclass:class}}
                h.addAnswers(m)
                if m.Rcode!=dns.RcodeRefused || len(m.Answer)!=0 {t.Fatalf("forwarded question not refused: type=%d name=%q class=%d rcode=%d",queryType,name,class,m.Rcode)}
            }
        }
    }
}

func TestTankReplayLocalZoneAndMixedQuestionHandling(t *testing.T) {
    zones:=[]types.Zone{{Name:"boxlite.internal.",Records:[]types.Record{{Name:"host",IP:net.ParseIP("192.168.127.254")}}}}
    h:=dnsHandler{zones:zones,forwardingDisabled:true}
    m:=new(dns.Msg);m.SetQuestion("host.boxlite.internal.",dns.TypeA);h.addAnswers(m)
    if len(m.Answer)!=1 || m.Rcode!=dns.RcodeSuccess {t.Fatal("built-in local DNS broke")}
    m=new(dns.Msg);m.Question=[]dns.Question{{Name:"host.boxlite.internal.",Qtype:dns.TypeA,Qclass:dns.ClassINET},{Name:"exfil.attacker.invalid.",Qtype:dns.TypeTXT,Qclass:dns.ClassINET}};h.addAnswers(m)
    if m.Rcode!=dns.RcodeRefused || len(m.Answer)!=0 {t.Fatal("local first question bypassed forwarding policy")}
}

func TestTankReplayConstructorDefaultsAndOpCodes(t *testing.T) {
    original,_:=New(nil,nil,nil)
    if original.handler.forwardingDisabled {t.Fatal("legacy unrestricted constructor changed")}
    restricted,_:=NewWithForwardingDisabled(nil,nil,nil,true)
    for _, opcode:=range []int{dns.OpcodeQuery,dns.OpcodeNotify,dns.OpcodeUpdate} {
        m:=new(dns.Msg);m.SetQuestion("exfil.attacker.invalid.",dns.TypeTXT);m.Opcode=opcode;restricted.handler.addAnswers(m)
        if m.Rcode!=dns.RcodeRefused {t.Fatalf("opcode %d bypassed forwarding policy",opcode)}
    }
}

// Exercise the real UDP and TCP response path too, without opening a listener.
type tankResponseWriter struct { result *dns.Msg }
func (w *tankResponseWriter) LocalAddr() net.Addr { return &net.UDPAddr{} }
func (w *tankResponseWriter) RemoteAddr() net.Addr { return &net.UDPAddr{} }
func (w *tankResponseWriter) WriteMsg(m *dns.Msg) error { w.result=m.Copy(); return nil }
func (w *tankResponseWriter) Write(b []byte) (int,error) { return len(b),nil }
func (w *tankResponseWriter) Close() error { return nil }
func (w *tankResponseWriter) TsigStatus() error { return nil }
func (w *tankResponseWriter) TsigTimersOnly(bool) {}
func (w *tankResponseWriter) Hijack() {}
func TestTankReplayUDPAndTCPRefuseExternalDNS(t *testing.T) {
    h:=dnsHandler{forwardingDisabled:true}
    for _, tcp:=range []bool{false,true} {
        for _, queryType:=range []uint16{dns.TypeA,dns.TypeTXT,dns.TypeAXFR,65535} {
            m:=new(dns.Msg);m.SetQuestion("exfil.attacker.invalid.",queryType)
            w:=&tankResponseWriter{}
            if tcp { h.handleTCP(w,m) } else { h.handleUDP(w,m) }
            if w.result==nil || w.result.Rcode!=dns.RcodeRefused || len(w.result.Answer)!=0 { t.Fatalf("transport tcp=%v type=%d did not refuse",tcp,queryType) }
        }
    }
}
