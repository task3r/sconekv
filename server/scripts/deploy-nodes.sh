docker service ls | grep -q tracker

if [ ! $? ]; then 
    docker service create --name tracker --network sconenet tracker
else
    docker run --rm -it --network sconenet curlimages/curl http://tracker:4321/clear
fi

osascript -e '
tell application "iTerm"
    activate

    # Create new tab
    tell current window
        create tab with default profile
    end tell

    # Split pane
    tell current session of current window
        split vertically with default profile
        split horizontally with default profile
    end tell
    tell third session of current tab of current window
        split horizontally with default profile
    end tell

    # Exec commands
    tell first session of current tab of current window
        write text "docker run --rm -it --network sconenet task3r/sconekv-node"
    end tell
    delay 1
    tell second session of current tab of current window
        write text "docker run --rm -it --network sconenet task3r/sconekv-node"
    end tell
    delay 1
    tell third session of current tab of current window
        write text "docker run --rm -it --network sconenet task3r/sconekv-node"
    end tell
    delay 1
    tell fourth session of current tab of current window
        write text "docker run --rm -it --network sconenet task3r/sconekv-node"
    end tell
end tell
'
