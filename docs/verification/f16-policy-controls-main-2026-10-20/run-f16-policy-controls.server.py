"""Disclosed synthetic rule controls, separate from the F16 historical packet."""
from pathlib import Path
from copy import deepcopy
import hashlib, importlib.util, json
from tank.analysis.decisions import DecisionEngine
from tank.replay import digest
from tank.util import iso
ROOT=Path("/srv/tank");HERE=Path(__file__).resolve().parent;OUT=HERE/"f16-policy-controls"
def main():
 OUT.mkdir(exist_ok=False)
 spec=importlib.util.spec_from_file_location("decision_scenarios",ROOT/"tests/test_decisions.py")
 module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
 event,assessment,beneficiary,market=module.inputs()
 controls=[]
 for name,patch,grades,action in [
  ("GRADE_A",{},["A","B"],"WATCH_TO_START"),
  ("GRADE_B",{"liquidity":{"liquidity_state":"THIN"}},["B","C"],"WAIT"),
  ("GRADE_C",{"move_class":"PERP_LED_CHASE"},["C","D"],"DONT_CHASE"),
  ("GRADE_D",{"data_confidence":{"class":"LOW"}},["D","D"],"WAIT"),
  ("OPEN_CALL_HOLD",{},["A","B"],"HOLD"),
  ("FATAL_EXIT",{},["A","B"],"EXIT"),
  ("STRUCTURAL_START",{},["A","A"],"START"),
  ("STRUCTURAL_ADD",{},["A","A"],"ADD"),
 ]:
  e,a,b,m=deepcopy((event,assessment,beneficiary,market));m.update(patch);extra={}
  if name in {"OPEN_CALL_HOLD","FATAL_EXIT","STRUCTURAL_ADD"}:extra["open_call"]={"invalidation_price":"9"}
  if name=="FATAL_EXIT":e["contradictions"]=[{"type":"FATAL"}]
  if name.startswith("STRUCTURAL"):
   a["class"]="STRUCTURAL_SHOCK";m["relative"]["windows"]={"EVENT":{"r_btc_adj":"1"}}
  if name=="STRUCTURAL_ADD":extra["conditions"]=[{"status":"FIRED","role":"UPGRADE"}]
  controls.append({"name":name,"declared_synthetic":True,"event":e,"assessment":a,"beneficiary":b,"market":m,"extra":extra,"expected":{"grades":grades,"stress_action":action}})
 inputs=[{k:v for k,v in c.items() if k!="expected"} for c in controls]
 seal={"sealed_at":iso(),"scope":"SYNTHETIC_POLICY_CONTROLS_NOT_HISTORICAL_F16_NEWS_OR_MARKET","inputs_sha256":digest(inputs),"assertion_spec":next(c for c in json.loads((ROOT/"tests/fixtures/manifest.json").read_text())["fixtures"] if c["fixture_id"]=="F-16")["assertion_spec"],"expectations":[{"name":c["name"],**c["expected"],"non_structural_never_bullish":not c["name"].startswith("STRUCTURAL")} for c in controls]}
 (OUT/"inputs.json").write_text(json.dumps(inputs,indent=2)+"\n")
 (OUT/"sealed-expectations.json").write_text(json.dumps(seal,indent=2)+"\n")
 outputs=[];engine=DecisionEngine()
 for c in inputs:
  m=deepcopy(c["market"]);m["relative"]["market_context"]="NEUTRAL"
  neutral=engine.evaluate(c["event"],c["assessment"],c["beneficiary"],m,**c["extra"])
  m["relative"]["market_context"]="STRESS"
  stress=engine.evaluate(c["event"],c["assessment"],c["beneficiary"],m,**c["extra"])
  outputs.append({"name":c["name"],"neutral":neutral,"stress":stress})
 frozen={"finished_at":iso(),"outputs":outputs,"output_sha256":digest(outputs),"decision_engine_source_sha256":hashlib.sha256((ROOT/"src/tank/analysis/decisions.py").read_bytes()).hexdigest(),"scope":"SYNTHETIC_ONLY_NOT_HISTORICAL_ACCEPTANCE_OR_NATIVE_WORDING"}
 frozen_path=OUT/"frozen-output.json";frozen_path.write_text(json.dumps(frozen,indent=2)+"\n")
 sealed=json.loads((OUT/"sealed-expectations.json").read_text())
 checks=[]
 for actual,want in zip(outputs,sealed["expectations"],strict=True):
  checks.append({"name":actual["name"],"grades":[actual["neutral"]["trade_grade"],actual["stress"]["trade_grade"]],"stress_action":actual["stress"]["action"],"stress_direction":actual["stress"]["outlook"]["direction"],"passed":([actual["neutral"]["trade_grade"],actual["stress"]["trade_grade"]]==want["grades"] and actual["stress"]["action"]==want["stress_action"] and (not want["non_structural_never_bullish"] or actual["stress"]["outlook"]["direction"]!="BULLISH"))})
 result={"scope":frozen["scope"],"checks":checks,"passed":all(c["passed"] for c in checks),"historical_f16_packet_unchanged":True,"native_wording_phase":"NOT_RUN","full_fixture_acceptance":False}
 (OUT/"evaluation.json").write_text(json.dumps(result,indent=2)+"\n");print(json.dumps(result,indent=2))
if __name__=="__main__":main()
