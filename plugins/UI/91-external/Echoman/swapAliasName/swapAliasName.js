(function () {
    "use strict";

    let svgBTN =
      '<svg fill="#ffffff" viewBox="0 0 256 256" width="18px" height="18px" id="Flat" xmlns="http://www.w3.org/2000/svg" stroke="#ffffff"><g id="SVGRepo_bgCarrier" stroke-width="0"></g><g id="SVGRepo_tracerCarrier" stroke-linecap="round" stroke-linejoin="round"></g><g id="SVGRepo_iconCarrier"> <path d="M176,200v8a16.01833,16.01833,0,0,1-16,16H48a16.01833,16.01833,0,0,1-16-16V104A16.01833,16.01833,0,0,1,48,88h96V71.99316a8.00037,8.00037,0,0,1,13.65674-5.65722l24,24a7.9885,7.9885,0,0,1,1.36011,1.822c.00622.01123.01367.022.01977.03332.10425.19275.19653.39.28369.58912.01856.04211.04.08215.05786.12463.0752.17956.13843.3623.2.5459.02247.06714.04908.13232.06995.20019.0531.17383.09436.34986.13538.52625.01867.08044.04223.15881.05859.24011.03809.19043.06348.38232.08765.57446.00879.06934.023.137.02978.20691.02637.26575.04053.53235.04041.79907L184,96c0,.025-.00342.04907-.00366.0741-.00244.23571-.0127.47131-.03577.70605-.01526.15759-.043.31128-.06726.46619-.01587.09985-.02612.20032-.04565.29968-.03565.1814-.083.358-.13062.53479-.01953.072-.03418.145-.05566.21655-.05567.18555-.12232.36573-.1908.54541-.0238.06226-.04333.12574-.06872.1875-.07715.18848-.165.37073-.256.55164-.02551.05066-.04712.10266-.0736.15283-.1084.20459-.22706.40222-.35218.59583-.0155.024-.02844.04907-.04431.073a8.0284,8.0284,0,0,1-1.09057,1.31836l-23.92847,23.92847A8.00037,8.00037,0,0,1,144,119.99316V104H48V208H160v-8a8,8,0,0,1,16,0ZM208,31.99316H96a16.01833,16.01833,0,0,0-16,16v8a8,8,0,0,0,16,0v-8H208v104H112V136a8.00037,8.00037,0,0,0-13.65674-5.65723L74.41479,154.27124a8.02882,8.02882,0,0,0-1.09057,1.31836c-.01587.02393-.02881.049-.04431.073-.12512.1936-.24378.39123-.35218.59582-.02636.04993-.048.10181-.07324.15222-.09106.1814-.1792.364-.25659.55286-.0249.06067-.04407.1228-.06738.184-.06885.18054-.136.36182-.1919.54834-.02148.07153-.03613.14453-.05566.21655-.04761.17676-.095.3534-.13062.53479-.01953.09937-.02978.19983-.04565.29969-.02429.1549-.052.30859-.06726.46618-.02307.23474-.03333.47034-.03577.70606-.00024.025-.00366.04907-.00366.07409l.00012.00208c-.00012.26672.014.53332.04041.79907.00683.07.021.13757.02978.20691.02417.19214.04956.384.08765.57446.01636.0813.03992.15967.05859.24012.041.17639.08228.35241.13538.52624.021.06836.04773.134.07043.20154.06128.18286.12439.36511.19922.54407.01807.04333.04029.08459.05921.12768.087.19812.17883.39453.28259.58655.0061.01135.01355.02209.01977.03333a7.98872,7.98872,0,0,0,1.36011,1.822l24,24A8.00038,8.00038,0,0,0,112,184V167.99316h96a16.01833,16.01833,0,0,0,16-16v-104A16.01833,16.01833,0,0,0,208,31.99316Z"></path> </g></svg>';
  
    function setNativeValue(element, value) {
      const valueSetter = Object.getOwnPropertyDescriptor(element, "value")?.set;
      const prototype = Object.getPrototypeOf(element);
      const prototypeValueSetter = Object.getOwnPropertyDescriptor(
        prototype,
        "value"
      )?.set;

      if (prototypeValueSetter && valueSetter !== prototypeValueSetter) {
        prototypeValueSetter.call(element, value);
      } else if (valueSetter) {
        valueSetter.call(element, value);
      } else {
        throw new Error("The given element does not have a value setter");
      }
  
      const eventName = element instanceof HTMLSelectElement ? "change" : "input";
      element.dispatchEvent(new Event(eventName, { bubbles: true }));
    }

    function swapText(nameInputElem, aliasGroupInputElem) {
      const nameSwapText = nameInputElem.value;
      const aliasInputElem = aliasGroupInputElem.querySelector("input");
      const aliasSwapText = aliasInputElem.value;
      setNativeValue(nameInputElem, aliasSwapText);
      setNativeValue(aliasInputElem, nameSwapText);
    }

    function buildButtons(nameInputElem, aliasGroupInputElem) {
      const getClass = aliasGroupInputElem.getElementsByClassName("aliasSwap");
      if (getClass.length === 0) {
        const svgContainerDiv = document.createElement("div");
        svgContainerDiv.classList.add("input-group-append", "aliasSwap");
        const btn = document.createElement("btn");
        btn.classList.add("btn", "btn-success");
        btn.setAttribute("type", "button");
        const svgElement = document.createElement("div");
        svgElement.innerHTML = svgBTN;
        btn.appendChild(svgElement);
        svgContainerDiv.appendChild(btn);
        aliasGroupInputElem.appendChild(svgContainerDiv);
        btn.addEventListener("click", function (event) {
          swapText(nameInputElem, aliasGroupInputElem);
        });
      }
    }

    function mutate(nameInputElem) {
      const observer = new MutationObserver((mutations) => {
        mutations.forEach((mutation) => {
          Array.from(mutation.addedNodes).forEach((addedNode) => {
            if (addedNode.matches && addedNode.matches(".input-group")) {
              setTimeout(function () {
                buildButtons(nameInputElem, addedNode);
              }, 500);
            }
          });
        });
      });
      observer.observe(document.body, {
        childList: true,
        subtree: true,
      });
    }

    stash.addEventListener("page:performer:details", function () {
      waitForElementClass("form-group", function () {
        const nameInputElem = document.querySelector("input#name");
        const aliasGroup = document.querySelector('div.form-group:has(>label[for="aliases"])');
        const inputGroupDivs = aliasGroup.querySelectorAll("div.input-group");
        inputGroupDivs.forEach(function (inputGroupDiv) {
            buildButtons(nameInputElem, inputGroupDiv);
        });
        setTimeout(function () {
          mutate(nameInputElem);
        }, 2000);
      });
    });
  })();