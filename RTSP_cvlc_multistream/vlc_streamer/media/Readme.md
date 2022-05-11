>longhelp
>+----[ Remote control commands ]
>|
>| add XYZ  . . . . . . . . . . . . add XYZ to playlist
>| enqueue XYZ  . . . . . . . . . queue XYZ to playlist
>| playlist . . . . .  show items currently in playlist
>| play . . . . . . . . . . . . . . . . . . play stream
>| stop . . . . . . . . . . . . . . . . . . stop stream
>| next . . . . . . . . . . . . . .  next playlist item
>| prev . . . . . . . . . . . .  previous playlist item
>| goto . . . . . . . . . . . . . .  goto item at index
>| repeat [on|off] . . . .  toggle playlist item repeat
>| loop [on|off] . . . . . . . . . toggle playlist loop
>| random [on|off] . . . . . . .  toggle random jumping
>| clear . . . . . . . . . . . . . . clear the playlist
>| status . . . . . . . . . . . current playlist status
>| title [X]  . . . . . . set/get title in current item
>| title_n  . . . . . . . .  next title in current item
>| title_p  . . . . . .  previous title in current item
>| chapter [X]  . . . . set/get chapter in current item
>| chapter_n  . . . . . .  next chapter in current item
>| chapter_p  . . . .  previous chapter in current item
>|
>| seek X . . . seek in seconds, for instance `seek 12'
>| pause  . . . . . . . . . . . . . . . .  toggle pause
>| fastforward  . . . . . . . .  .  set to maximum rate
>| rewind  . . . . . . . . . . . .  set to minimum rate
>| faster . . . . . . . . . .  faster playing of stream
>| slower . . . . . . . . . .  slower playing of stream
>| normal . . . . . . . . . .  normal playing of stream
>| f [on|off] . . . . . . . . . . . . toggle fullscreen
>| info . . . . .  information about the current stream
>| stats  . . . . . . . .  show statistical information
>| get_time . . seconds elapsed since stream's beginning
>| is_playing . . . .  1 if a stream plays, 0 otherwise
>| get_title . . . . .  the title of the current stream
>| get_length . . . .  the length of the current stream
>|
>| volume [X] . . . . . . . . . .  set/get audio volume
>| volup [X]  . . . . . . .  raise audio volume X steps
>| voldown [X]  . . . . . .  lower audio volume X steps
>| adev [X] . . . . . . . . . . .  set/get audio device
>| achan [X]. . . . . . . . . .  set/get audio channels
>| atrack [X] . . . . . . . . . . . set/get audio track
>| vtrack [X] . . . . . . . . . . . set/get video track
>| vratio [X]  . . . . . . . set/get video aspect ratio
>| vcrop [X]  . . . . . . . . . . .  set/get video crop
>| vzoom [X]  . . . . . . . . . . .  set/get video zoom
>| snapshot . . . . . . . . . . . . take video snapshot
>| strack [X] . . . . . . . . . set/get subtitles track
>| key [hotkey name] . . . . . .  simulate hotkey press
>| menu . . [on|off|up|down|left|right|select] use menu
>|
>| @name marq-marquee  STRING  . . overlay STRING in video
>| @name marq-x X . . . . . . . . . . . .offset from left
>| @name marq-y Y . . . . . . . . . . . . offset from top
>| @name marq-position #. . .  .relative position control
>| @name marq-color # . . . . . . . . . . font color, RGB
>| @name marq-opacity # . . . . . . . . . . . . . opacity
>| @name marq-timeout T. . . . . . . . . . timeout, in ms
>| @name marq-size # . . . . . . . . font size, in pixels
>|
>| @name logo-file STRING . . .the overlay file path/name
>| @name logo-x X . . . . . . . . . . . .offset from left
>| @name logo-y Y . . . . . . . . . . . . offset from top
>| @name logo-position #. . . . . . . . relative position
>| @name logo-transparency #. . . . . . . . .transparency
>|
>| @name mosaic-alpha # . . . . . . . . . . . . . . alpha
>| @name mosaic-height #. . . . . . . . . . . . . .height
>| @name mosaic-width # . . . . . . . . . . . . . . width
>| @name mosaic-xoffset # . . . .top left corner position
>| @name mosaic-yoffset # . . . .top left corner position
>| @name mosaic-offsets x,y(,x,y)*. . . . list of offsets
>| @name mosaic-align 0..2,4..6,8..10. . .mosaic alignment
>| @name mosaic-vborder # . . . . . . . . vertical border
>| @name mosaic-hborder # . . . . . . . horizontal border
>| @name mosaic-position {0=auto,1=fixed} . . . .position
>| @name mosaic-rows #. . . . . . . . . . .number of rows
>| @name mosaic-cols #. . . . . . . . . . .number of cols
>| @name mosaic-order id(,id)* . . . . order of pictures
>| @name mosaic-keep-aspect-ratio {0,1} . . .aspect ratio
>|
>| help . . . . . . . . . . . . . . . this help message
>| longhelp . . . . . . . . . . . a longer help message
>| logout . . . . . . .  exit (if in socket connection)
>| quit . . . . . . . . . . . . . . . . . . .  quit vlc
>|
>+----[ end of help ]

"""
